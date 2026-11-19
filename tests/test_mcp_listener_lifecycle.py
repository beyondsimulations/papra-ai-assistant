"""Task 12 fix round: the listener's availability half, and the flags no in-process test sees.

`tests/test_mcp_server_auth.py` and `tests/test_mcp_server_status.py` prove what the listener
*answers*. This file is about the other question the review asked, which none of them could:
whether anyone is watching the process that answers.

Four things are pinned here, each because its absence was invisible rather than loud:

**A dead listener is an event** (review I1). The old code created the uvicorn task and then awaited
only `stop`, which `serve` always passes, so a uvicorn that died — `address already in use` raises
`SystemExit` inside `Server.startup` — left the `mcp` task alive forever, `_supervise` silent, and
`check` printing `mcp: enabled`.

**The uvicorn it builds is the one the deployment needs** (I2, plus the unpinned `access_log`).
`proxy_headers` is what makes a slash redirect behind `tailscale serve` come back as `https://`, and
`access_log=False` is the single flag between the spec's measured token-in-stdout leak and reality.
`TestClient` never runs uvicorn, so both had to be asserted where the config object is constructed.

**A credential from another process is still redacted.** The verify path registers the plaintext it
accepts; the test that appeared to prove it minted the token in-process, where the registry already
knew the value. Minting out-of-band is the only shape that pins the line.

**`PAPRA_MCP_HOST` in the forms an operator pastes.** `resolve_bind` supports `::1`; the only way to
an operator is through `load_config`, which is where the bracketed form used to die.
"""

import logging
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from papra_suite.mcp_server import MCP_ENABLED_SETTING
from papra_suite.store import Store
from tests.test_config import BASE_ENV
from tests.test_mcp_server_auth import OkKreuzakt, OkPapra, app_for, client_for, initialize

PUBLIC_URL = "https://papra-mcp.ts.net"
ORG = "org-1"

#: The environment `serve` needs, with values long enough not to be redaction hazards: `serve`
#: registers every secret it is given with the suite's redaction filter, and a one-character API key
#: (`"k"`, in the shorter fixture this one replaced) scrubs that letter out of **every** line the
#: process writes afterwards — which is what hid the crash line from the assertion below.
SERVE_ENV = {
    "PAPRA_ALLOWED_SENDERS": "sender@example.com",
    "PAPRA_BASE_URL": "http://papra:1221", "PAPRA_API_KEY": "papra-key-1",
    "LLM_BASE_URL": "http://llm:8000", "LLM_API_KEY": "llm-key-1",
    "TITLE_MODEL": "title-model", "OCR_MODEL": "ocr-model", "ADMIN_PASSWORD": "admin-password",
    "SWEEP_INTERVAL_S": "0",
    "PAPRA_KREUZAKT_URL": "http://kreuzakt.test",
    "PAPRA_MCP_URL": PUBLIC_URL,
}


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    yield store
    store.close()


class DyingUvicorn:
    """A `uvicorn.Server` whose `serve()` dies on the way up, exactly as a taken port does.

    `Server.startup` calls `sys.exit(1)` when the bind fails, so the coroutine `run_mcp_listener`
    created ends with `SystemExit`, which is not an Exception: neither `except Exception` nor a
    plain `await` of a sibling task ever sees it, and it punches through the event loop itself.
    """

    def __init__(self, config):
        self.config = config
        self.should_exit = False

    async def serve(self):
        raise SystemExit(1)


def listener_config(tmp_path, port=8765):
    return SimpleNamespace(mcp_host="127.0.0.1", mcp_port=port, mcp_url=PUBLIC_URL,
                           data_dir=tmp_path)


# --- a listener that dies has to end the task that runs it (I1) -----------------------------


async def test_a_listener_that_dies_ends_the_mcp_task(monkeypatch, tmp_path, store, caplog):
    """The dead-listener bug in one assertion: FIRST_COMPLETED on `{serving, stop}`.

    Awaiting only `stop` — which `serve` always passes — meant the `mcp` task never completed, so
    `_supervise`'s done-callback never fired and nothing was logged.
    """
    import asyncio

    import uvicorn

    from papra_suite.mcp_server import McpListenerDied, run_mcp_listener

    monkeypatch.setattr(uvicorn, "Server", DyingUvicorn)
    stop = asyncio.Event()
    with caplog.at_level(logging.INFO, logger="papra_suite.mcp_server"):
        task = asyncio.create_task(run_mcp_listener(listener_config(tmp_path), store,
                                                    papra=OkPapra(), kreuzakt=OkKreuzakt(),
                                                    stop=stop))
        with pytest.raises(McpListenerDied):
            await asyncio.wait_for(task, timeout=5.0)

    assert stop.is_set(), "a dead listener must bring the process down with it"
    # Nothing this process logs before uvicorn returns may claim a socket it does not hold: the
    # everyday death is a port somebody else already has, and a line that said "listening" one line
    # above the crash is read as "it came up, so the problem is somewhere else".
    logged = [r.getMessage() for r in caplog.records if "mcp_server" in r.name]
    assert logged, "nothing was logged, so the assertion below would prove nothing"
    claimed = [m for m in logged if "listening" in m]
    assert not claimed, f"the startup line reported a bind that was never confirmed: {claimed}"


async def test_a_dead_listener_takes_serve_down_with_it(monkeypatch, tmp_path, store):
    """Fatal to `serve`, deliberately, rather than restarted inside the process.

    `serve` is the unit the container supervisor restarts and the unit that owns the store, the
    clients and the signal handlers. An in-process restart loop would keep a misconfiguration (a
    port somebody else holds) alive behind a stream of retry lines, would re-enter uvicorn's SIGTERM
    handler installation, and — worst — would let the process go on advertising a listener that is
    not listening. Exiting is the honest answer, and the restart policy is the backoff.
    """
    import asyncio

    import uvicorn

    from papra_suite.mcp_server import McpListenerDied, run_mcp_listener

    monkeypatch.setattr(uvicorn, "Server", DyingUvicorn)
    stop = asyncio.Event()
    task = asyncio.create_task(run_mcp_listener(listener_config(tmp_path), store,
                                                papra=OkPapra(), kreuzakt=OkKreuzakt(), stop=stop))

    with pytest.raises(McpListenerDied):
        await asyncio.wait_for(task, timeout=5.0)
    assert stop.is_set(), "a dead listener must bring the process down with it"


async def test_a_dead_listener_is_a_crash_the_supervisor_reports(monkeypatch, tmp_path, store,
                                                                 capsys):
    """The other half: `serve` must not run on with no listener and a quiet log.

    Same shape as a crashed poller, so it goes through the real `serve`: the task raises,
    `_supervise` logs it as a crash, and the stop that `serve` waits on is set.
    """
    import asyncio

    from papra_suite.config import load_config
    from papra_suite.pipeline import serve
    from tests.test_index_job import FakeKreuzakt, FakePapra

    async def dying(config, store, *, papra, kreuzakt, stop=None):
        stop.set()
        raise RuntimeError("address already in use")

    monkeypatch.setattr("papra_suite.mcp_server.run_mcp_listener", dying)
    store.set_setting(MCP_ENABLED_SETTING, "1")
    config = load_config({**SERVE_ENV, "DATA_DIR": str(tmp_path)})
    stop = asyncio.Event()

    # `serve` calls `setup_logging`, which replaces the root handlers: without putting them
    # back, the handler it installed points at this test's captured stream, and every log line the
    # suite writes afterwards fails on a closed file and lands on someone else's stderr.
    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
    try:
        await serve(config, stop=stop, store=store, papra=FakePapra(), kreuzakt=FakeKreuzakt())
    finally:
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)

    # Read from the process's own sink, not `caplog`: `serve` calls `setup_logging`, which
    # root handlers, and the handler `caplog` had installed is cleared with them.
    printed = capsys.readouterr().out
    assert "exited unexpectedly" in printed
    assert "address already in use" in printed


# --- the uvicorn the listener actually builds (I2, and the unpinned `access_log`) -----------


def test_the_listener_config_trusts_the_proxy_and_writes_no_access_log():
    """Two flags no in-process test can observe, pinned where the config object is built.

    `proxy_headers`: behind `tailscale serve` a slash redirect (`/mcp` ↔ `/mcp/`) is built from the
    inbound scheme, so without it the server answers a proxied client with `http://` and the client
    refuses the downgrade — green on localhost, dead in production. The 2026-11-16 spec lists it as
    a deployment item for that reason, and it cannot weaken the 421 gate: uvicorn's
    `ProxyHeadersMiddleware` rewrites `scope["scheme"]` and `scope["client"]`, never `host`.

    `access_log`: uvicorn's access log goes to **stdout** with the full query string, which is the
    one place a refused `?access_token=` could be written verbatim. `TestClient` never runs uvicorn,
    so flipping this flag used to leave the whole suite green and README's claim false.
    """
    import uvicorn

    from papra_suite.mcp_server import FORWARDED_ALLOW_IPS, McpBind, uvicorn_config_kwargs

    kwargs = uvicorn_config_kwargs(McpBind(host="127.0.0.1", port=8765))

    assert kwargs["access_log"] is False
    assert kwargs["proxy_headers"] is True
    assert kwargs["forwarded_allow_ips"] == FORWARDED_ALLOW_IPS
    assert "*" not in FORWARDED_ALLOW_IPS
    assert "0.0.0.0" not in FORWARDED_ALLOW_IPS

    config = uvicorn.Config(object, **kwargs)      # the real object, not a dict we invented
    assert config.access_log is False
    assert config.proxy_headers is True
    assert config.forwarded_allow_ips == FORWARDED_ALLOW_IPS


def test_an_ambient_forwarded_allow_ips_cannot_widen_the_trust_list(monkeypatch):
    """uvicorn reads `FORWARDED_ALLOW_IPS` from the environment when the code leaves it unset.

    One `FORWARDED_ALLOW_IPS=*` in a container environment would make every peer's
    `X-Forwarded-For` authoritative. Passing the value explicitly closes that, and it is what stops
    an uvicorn default changing underneath us: this deployment trusts loopback, because
    `tailscale serve` is the only peer.
    """
    import uvicorn

    from papra_suite.mcp_server import McpBind, uvicorn_config_kwargs

    monkeypatch.setenv("FORWARDED_ALLOW_IPS", "*")
    config = uvicorn.Config(object, **uvicorn_config_kwargs(McpBind(host="127.0.0.1", port=8765)))

    assert "*" not in str(config.forwarded_allow_ips)


# --- a credential that arrived from elsewhere (the unpinned `SECRET_REGISTRY.add`) ----------


def mint_out_of_band(db_path, label, org_ids):
    """Mint a token in a **separate process**, so this one has never seen the plaintext.

    `StoreTokenVerifier` registers every token it accepts with the redaction filter, and the test
    that appeared to prove it minted in-process — where the registry already knew the value, so
    deleting that line changed nothing. A token that reaches a real server was always minted
    somewhere else.
    """
    root = str(Path(__file__).resolve().parents[1])
    code = (
        "import sys; sys.path.insert(0, sys.argv[1]);"
        "from papra_suite.mcp_tokens import mint_token;"
        "from papra_suite.store import Store;"
        "from pathlib import Path;"
        "store = Store(Path(sys.argv[2])); store.migrate();"
        "print(mint_token(store, label=sys.argv[3], org_ids=sys.argv[4:]))"
    )
    finished = subprocess.run([sys.executable, "-c", code, root, str(db_path), label, *org_ids],
                              capture_output=True, text=True, check=True, timeout=120)
    return finished.stdout.strip().splitlines()[-1]


def test_a_token_that_arrived_from_another_process_is_still_redacted(tmp_path, capsys):
    """The verifier is where a foreign credential joins the redaction filter."""
    from papra_suite.logging_setup import REDACTED, SECRET_REGISTRY, setup_logging

    token = mint_out_of_band(tmp_path / "state.db", "hermes", [ORG])
    assert token not in SECRET_REGISTRY._secrets, "the mint leaked into this process's registry"

    store = Store(tmp_path / "state.db")
    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
    setup_logging()
    try:
        with client_for(app_for(store)) as client:
            initialize(client, token)          # the only place this value is ever seen here
        logging.getLogger("some-http-client").info("GET /mcp?access_token=%s", token)
    finally:
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)
        store.close()

    printed = capsys.readouterr().out
    assert token not in printed, "a token first seen at the gate reached a log line in the clear"
    assert REDACTED in printed


# --- `PAPRA_MCP_HOST` in the forms an operator pastes ---------------------------------------


def test_the_ipv6_loopback_host_is_configurable_through_the_environment():
    """`::1` and `[::1]` are the same literal, and both are on `LOOPBACK_HOSTS`.

    A test that hands `resolve_bind` a `SimpleNamespace` cannot see this: the only way an operator
    reaches `resolve_bind` is through `load_config`, and both spellings have to survive it, because
    uvicorn wants the bare form and `curl` wants the bracketed one.
    """
    from papra_suite.config import load_config
    from papra_suite.mcp_server import resolve_bind

    for form in ("::1", "[::1]"):
        config = load_config(dict(BASE_ENV, PAPRA_MCP_HOST=form, PAPRA_MCP_URL=PUBLIC_URL))
        assert config.mcp_host == "::1", form
        bind = resolve_bind(config)          # loopback: no settings row needed
        assert bind.host == "::1" and str(bind) == "[::1]:8765"


def test_a_refused_bind_address_does_not_echo_what_was_typed():
    """The refusal for a bad `PAPRA_MCP_HOST` is printed to **stderr**, with no filter in front.

    Every sibling in `config.py` — `PAPRA_BASE_URL`, `DASHBOARD_BIND`, the Kreuzakt URL — echoes
    through `_safe_echo` for exactly this reason, and this one field was the exception. The value
    that arrives here is whatever an operator pasted into an interface field, and the paste people
    actually make is the URL from a browser bar, which can carry `user:password@`.
    """
    from papra_suite.config import ConfigError, load_config

    with pytest.raises(ConfigError) as caught:
        load_config({**BASE_ENV, "PAPRA_MCP_HOST": "https://user:supersecret@example.com"})

    message = str(caught.value)
    assert "supersecret" not in message and "user:" not in message
    assert "PAPRA_MCP_HOST" in message, "the refusal still has to name the variable"


def test_a_bracketed_host_that_is_not_an_ipv6_literal_is_refused():
    from papra_suite.config import ConfigError, load_config

    for bad in ("[papra-mcp.ts.net]", "[::1]:8765", "[::1"):
        with pytest.raises(ConfigError):
            load_config(dict(BASE_ENV, PAPRA_MCP_HOST=bad))
