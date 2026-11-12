import json
import logging

import pytest

from papra_suite.logging_setup import SECRET_REGISTRY, setup_logging
from papra_suite.store import Store


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


def test_secrets_are_redacted_everywhere(store, capsys):
    SECRET_REGISTRY.add("s3cret-password")
    setup_logging(store)
    logging.getLogger("test").info("login failed for s3cret-password at imap.example.com")
    line = capsys.readouterr().out.strip().splitlines()[-1]
    assert "s3cret-password" not in line
    assert "[REDACTED]" in json.loads(line)["message"]


def test_ring_buffer_receives_lines(store):
    setup_logging(store)
    logging.getLogger("test").warning("watch out")
    lines = store.recent_log_lines(limit=10)
    assert any("watch out" in line for _, _, line in lines)


def test_ring_buffer_lines_are_redacted(store):
    SECRET_REGISTRY.add("hunter2-hunter2")
    setup_logging(store)
    logging.getLogger("test").error("auth error, password=hunter2-hunter2")
    lines = store.recent_log_lines(limit=10)
    assert all("hunter2-hunter2" not in line for _, _, line in lines)


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
