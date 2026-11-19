"""Containment option C, availability half: a dead listener has to end the process.

`serve` hosts the MCP listener as one task among the pollers and the workers. Before this, a
listener that died left `serve` waiting on its own stop event: the port was gone, `check` kept
printing `mcp: enabled`, and when the process finally stopped it stopped with **exit 0** — which a
Dokploy `restart: on-failure` policy cannot see. A dead port that looks healthy from the outside is
the failure this file refuses: the process must exit non-zero and say, in one sentence, which task
died and why.

Scoped deliberately. Only the listener task is fatal: the scheduler's own behaviour — a poll loop
that exits is logged as a crash, a loop that stops by design logs INFO, the process carries on — is
left exactly as it was: `tests/test_serve.py::test_serve_logs_unexpected_task_exit` and
`test_serve_logs_designed_task_exit_as_info` are what pin that, and they pass unchanged here. Both
death shapes are covered: a task that
**raises** and a task that **returns normally** without ever being asked to stop are the same
outage from the outside, so they are the same exit here.
"""

import logging
import sys
from contextlib import contextmanager

import pytest

from papra_suite.mcp_server import MCP_ENABLED_SETTING, McpListenerDied
from papra_suite.store import Store
from tests.test_config import BASE_ENV

PUBLIC_URL = "https://papra-mcp.test"

#: Long enough not to be redaction hazards: `serve` registers every secret with the redaction
#: filter, and a one-character API key scrubs that letter out of every line the process writes.
SERVE_ENV = {
    "PAPRA_ALLOWED_SENDERS": "sender@example.com",
    "PAPRA_BASE_URL": "http://papra:1221", "PAPRA_API_KEY": "papra-key-1",
    "LLM_BASE_URL": "http://llm:8000", "LLM_API_KEY": "llm-key-1",
    "TITLE_MODEL": "title-model", "OCR_MODEL": "ocr-model", "ADMIN_PASSWORD": "admin-password",
    "SWEEP_INTERVAL_S": "0", "PAPRA_KREUZAKT_URL": "http://kreuzakt.test",
    "PAPRA_MCP_URL": PUBLIC_URL,
}


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    yield store
    store.close()


@contextmanager
def _restored_logging():
    """Put the root handlers back: `serve` calls `setup_logging`, which replaces them.

    Without this the handler `caplog` had installed is cleared with the others, and every later log
    line in the suite fails on a closed stream — a failure that lands on someone else's test.
    """
    root = logging.getLogger()
    saved, level = list(root.handlers), root.level
    try:
        yield
    finally:
        root.handlers[:] = saved
        root.setLevel(level)


async def _serve_until_it_ends(store, tmp_path, monkeypatch, listener):
    """Run `serve` with `run_mcp_listener` replaced by `listener`; return its stop event.

    Raises whatever `serve` raises — which, for a dead listener, is the point of the call.
    """
    import asyncio

    from papra_suite.config import load_config
    from papra_suite.pipeline import serve
    from tests.test_index_job import FakeKreuzakt, FakePapra

    monkeypatch.setattr("papra_suite.mcp_server.run_mcp_listener", listener)
    store.set_setting(MCP_ENABLED_SETTING, "1")
    config = load_config({**SERVE_ENV, "DATA_DIR": str(tmp_path)})
    stop = asyncio.Event()
    with _restored_logging():
        await serve(config, stop=stop, store=store, papra=FakePapra(), kreuzakt=FakeKreuzakt())
    return stop


async def test_a_listener_that_returns_normally_ends_serve_non_zero(store, tmp_path, monkeypatch):
    """A listener that came back on its own is an outage, not a shutdown.

    Nothing in `serve` asks the `mcp` task to stop while it runs: it waits on `stop`, and
    `run_mcp_listener` converts uvicorn's own exits into `McpListenerDied`. So a listener that
    returns has finished serving without being asked, and `serve` running on beside a port nobody
    holds is the silence this assertion replaces.
    """
    async def finished(config, store, *, papra=None, kreuzakt=None, stop=None):
        return None

    with pytest.raises(McpListenerDied) as caught:
        await _serve_until_it_ends(store, tmp_path, monkeypatch, finished)
    assert "mcp" in str(caught.value)


async def test_a_listener_that_raises_names_the_task_and_the_reason(store, tmp_path, monkeypatch):
    """The sentence has to carry the task and the cause, or `docker logs` says nothing.

    A traceback is printed by `asyncio.run` whatever happens here; what the operator needs above it
    is one line: which of the hosted tasks died, and the exception that killed it.
    """
    async def crashing(config, store, *, papra=None, kreuzakt=None, stop=None):
        raise OSError("cannot assign requested address")

    with pytest.raises(McpListenerDied) as caught:
        await _serve_until_it_ends(store, tmp_path, monkeypatch, crashing)
    sentence = str(caught.value)
    assert "mcp" in sentence and "cannot assign requested address" in sentence
    assert "\n" not in sentence, "one sentence, on one line, for the log"


def test_the_process_exits_one_when_the_listener_dies(tmp_path, monkeypatch, capsys):
    """`main` decides the exit code, and 0 is the answer that hides the outage.

    The shipped container recovers by restarting, and the policy that does it (`restart:
    on-failure`) looks only at that code. Everything above this line is invisible to it.
    """

    from papra_suite.__main__ import main
    from papra_suite.mcp_server import McpListenerDied

    async def dead_serve(config, *, stop=None, **kwargs):
        raise McpListenerDied("task mcp died: OSError: cannot assign requested address")

    monkeypatch.setattr("papra_suite.__main__.run_serve", dead_serve)
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "serve"])

    assert main() == 1, "a dead listener must not look like a clean shutdown"
    assert capsys.readouterr().err.strip() == (
        "fatal: task mcp died: OSError: cannot assign requested address")
