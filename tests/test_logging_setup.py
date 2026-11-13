import json
import logging

import pytest

from papra_suite.logging_setup import SECRET_REGISTRY, setup_logging
from papra_suite.store import Store


@pytest.fixture(autouse=True)
def _isolate_root_logger_and_secrets():
    """Snapshot the root logger's handlers and the secret registry before each
    test and restore them after, so setup_logging's handlers (especially the
    RingBufferHandler holding a closed store) cannot leak into later tests."""
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_secrets = set(SECRET_REGISTRY._secrets)
    yield
    root.handlers[:] = saved_handlers
    SECRET_REGISTRY._secrets.clear()
    SECRET_REGISTRY._secrets.update(saved_secrets)


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    yield store
    store.close()


def test_output_is_json_lines_with_expected_keys(store, capsys):
    setup_logging(store)
    logging.getLogger("test").info("hello %s", "world")
    line = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["level"] == "INFO"
    assert payload["logger"] == "test"
    assert payload["message"] == "hello world"
    assert "ts" in payload
    # Pin tz-awareness: a naive datetime.now() would emit a local-time ts
    # without a UTC offset.
    assert payload["ts"].endswith("+00:00")


def test_secrets_are_redacted_everywhere(store, capsys):
    SECRET_REGISTRY.add("s3cret-password")
    setup_logging(store)
    logging.getLogger("test").info("login failed for s3cret-password at imap.example.com")
    line = capsys.readouterr().out.strip().splitlines()[-1]
    assert "s3cret-password" not in line
    assert "[REDACTED]" in json.loads(line)["message"]


def test_short_secrets_are_redacted(store, capsys):
    SECRET_REGISTRY.add("abc")
    setup_logging(store)
    logging.getLogger("test").info("login with password=abc failed")
    raw = capsys.readouterr().out
    line = raw.strip().splitlines()[-1]
    assert "abc" not in raw
    assert "[REDACTED]" in json.loads(line)["message"]


def test_ring_buffer_receives_lines(store):
    setup_logging(store)
    logging.getLogger("test").warning("watch out")
    lines = store.recent_log_lines(limit=10)
    assert any("watch out" in line for _, _, line in lines)
    # Pin the ring buffer's ts tz-awareness, like the stdout sink's.
    assert all(ts.endswith("+00:00") for ts, _, _ in lines)
    # Pin the level: the WARNING record must be stored as "WARNING", not a
    # hardcoded "INFO".
    warning_entries = [(ts, level, line) for ts, level, line in lines if "watch out" in line]
    assert warning_entries and warning_entries[0][1] == "WARNING"


def test_json_output_keeps_unicode_readable(store, capsys):
    """ensure_ascii=False must be kept: non-ASCII characters (e.g. an en
    dash) must appear literally in the raw stdout JSON, not as a \u2013
    escape sequence."""
    setup_logging(store)
    logging.getLogger("test").info("Rechnung 42 \u2013 Titel")
    raw = capsys.readouterr().out
    line = raw.strip().splitlines()[-1]
    assert "\u2013" in line  # the literal en dash character
    assert "\\u2013" not in line  # ...not the escaped form


def test_ring_buffer_lines_are_redacted(store):
    SECRET_REGISTRY.add("hunter2-hunter2")
    setup_logging(store)
    logging.getLogger("test").error("auth error, password=hunter2-hunter2")
    lines = store.recent_log_lines(limit=10)
    assert all("hunter2-hunter2" not in line for _, _, line in lines)


def test_ring_buffer_never_raises_after_store_close(store, capsys):
    """After the store is closed, the RingBufferHandler's emit must swallow
    the ProgrammingError via its except/handleError guard — logging must
    never crash the app — while the stdout sink keeps emitting."""
    setup_logging(store)
    logging.getLogger("test").info("before close")
    store.close()

    logging.getLogger("test").info("after close")  # must not raise

    out = capsys.readouterr().out
    assert "before close" in out
    assert "after close" in out


def test_exception_text_is_redacted_in_both_sinks(store, capsys):
    SECRET_REGISTRY.add("sup3r-secret-token")
    setup_logging(store)
    try:
        raise ValueError("connection failed with sup3r-secret-token")
    except ValueError:
        logging.getLogger("test").error("boom", exc_info=True)

    stdout_line = capsys.readouterr().out.strip().splitlines()[-1]
    assert "sup3r-secret-token" not in stdout_line
    payload = json.loads(stdout_line)
    assert "sup3r-secret-token" not in payload["exc_info"]
    assert "[REDACTED]" in payload["exc_info"]

    ring_lines = store.recent_log_lines(limit=10)
    matching = [line for _, _, line in ring_lines if "boom" in line]
    assert matching, "expected the error line in the ring buffer"
    assert all("sup3r-secret-token" not in line for line in matching)
    assert any("[REDACTED]" in line for line in matching)
