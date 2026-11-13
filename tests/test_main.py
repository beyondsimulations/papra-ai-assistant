"""Tests pinning the `papra-suite check` command contract.

These call main() directly so mutations of the exit codes or of the
store.migrate() call fail a test.
"""

import sqlite3
import sys

from papra_suite.__main__ import main
from tests.test_config import BASE_ENV


def test_check_success_migrates_and_exits_zero(monkeypatch, tmp_path):
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "check"])

    assert main() == 0

    db_path = tmp_path / "state.db"
    assert db_path.exists()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        user_version = conn.execute("PRAGMA user_version").fetchone()[0]
    finally:
        conn.close()
    assert user_version == 1


def test_check_config_error_exits_two(monkeypatch, capsys, tmp_path):
    env = dict(BASE_ENV)
    del env["PAPRA_API_KEY"]
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "check"])

    assert main() == 2
    stderr = capsys.readouterr().err
    assert "configuration error" in stderr
    assert "PAPRA_API_KEY" in stderr
