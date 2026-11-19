"""The one legitimate write on the request path, and how narrow "narrow" has to be.

Containment option C (the 2026-11-19 amendment to the 2026-11-16 spec) says the request path is made
of objects that cannot write — and then names one exception: the token's `last_used_at`, "on its own
short-lived connection behind the existing 60-second throttle and never handed to a handler".

Two things belong on the record before the tests. The write did not exist: `git log --all -S
last_used_at` finds only the spec commit, there is no `mcp_tokens` table (a token is a JSON document
in `settings` under `mcp_token:<sha256hex>`), and there was no throttle to keep. So the path is
built here rather than moved, and it is built narrow on purpose:

* the stamp is **not** written into the token row, because `_parse` requires that row's key set
to be exactly five names and refuses anything else — a sixth key would silently stop every token
minted before this change from authenticating. That is the authorisation surface, not a
reporting field; * it is therefore its own settings family, `mcp_token_use:<sha256hex>`, which
the token enumeration cannot match (the prefixes differ at the eleventh character, `_` against
`:`), so a stamp can never be read as a credential; * it goes through its own `sqlite3.connect`
on the call, not through the `Store` and not through the `LedgerReader`: the view runs under
`PRAGMA query_only=ON` and could not write it if asked.

And the throttle is not politeness. It is the difference between one settings write per token per
minute and one per HTTP request — with the listener sharing `serve`'s single-writer WAL
database, the second version puts an agent's polling in front of delivery writes for a timestamp
nobody needs fresh.
"""

import hashlib
import sqlite3
from datetime import datetime

import pytest

from papra_suite import mcp_tokens
from papra_suite.mcp_tokens import (
    TOKEN_SETTING_PREFIX,
    TOKEN_USE_PREFIX,
    list_tokens,
    mint_token,
    resolve_token,
    revoke_token,
    stamp_token_use,
    token_counts,
)
from papra_suite.store import Store

ORG = "org-1"


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    yield store
    store.close()


@pytest.fixture(autouse=True)
def _empty_throttle():
    """The throttle is process state, so a test that counts writes owns the counter it counts on."""
    mcp_tokens._recent_stamps.clear()
    yield
    mcp_tokens._recent_stamps.clear()


@pytest.fixture
def connections(monkeypatch):
    """Every connection `mcp_tokens` opens, so "it did not write" is a countable thing.

    Patched on the module's own `_connect` seam rather than on `sqlite3.connect`: patching the
    stdlib attribute would count, and change, every connection in the process — including the
    `Store` a test opens to check the result.
    """
    from papra_suite import mcp_tokens

    opened = []
    real = mcp_tokens._connect

    def counting(path):
        conn = real(path)
        opened.append(conn)
        return conn

    monkeypatch.setattr(mcp_tokens, "_connect", counting)
    return opened


def use_rows(store):
    return {key: store.get_setting(key) for key in store.list_setting_keys()
            if key.startswith(TOKEN_USE_PREFIX)}


def use_key(raw):
    return TOKEN_USE_PREFIX + hashlib.sha256(raw.encode()).hexdigest()


def use(store, raw, **kwargs):
    """Stamp the way the auth path does: with the resolved context as proof, or nothing."""
    return stamp_token_use(store.db_path, raw, resolve_token(store, raw), **kwargs)


# --- the stamp exists, and only for a credential that worked ----------------------------------


def test_an_authenticated_token_records_that_it_was_used(store):
    token = mint_token(store, label="hermes", org_ids=[ORG])
    assert stamp_token_use(store.db_path, token, resolve_token(store, token)) is True

    rows = use_rows(store)
    assert list(rows) == [use_key(token)]
    assert rows[use_key(token)]
    assert token not in "".join(rows.values()), "the plaintext travelled into the database"


def test_a_token_that_did_not_work_is_never_stamped(store):
    """Nothing is written for a rejection: an anonymous stranger cannot fill the ledger.

    The resolved context is the proof, and its absence is the answer: a guessed bearer string
    costs a
    digest and no database work at all. Revoked resolves to None, so a dead credential cannot keep
    its own record current and argue against the operator's decision.
    """
    minted = mint_token(store, label="pi", org_ids=[ORG])
    revoke_token(store, label="pi")

    assert stamp_token_use(store.db_path, minted, None) is False
    assert stamp_token_use(store.db_path, "not-a-token-at-all", None) is False
    assert stamp_token_use(store.db_path, "", None) is False
    assert use_rows(store) == {}


def test_a_stamp_is_not_a_credential(store):
    """`mcp_token_use:` must never look like a token row to the code that reads tokens.

    The two prefixes differ by one character, and if they collided, a stamp written by an agent's
    request would become a row in `mcp-token --list` — and a row under `mcp_token:` is a
    credential by definition, so a throttle would be a way to mint scope from the network.
    """
    token = mint_token(store, label="pi", org_ids=[ORG])
    stamp_token_use(store.db_path, token, resolve_token(store, token))

    assert resolve_token(store, token) is not None
    assert [info.label for info in list_tokens(store)] == ["pi"]
    assert token_counts(store) == (1, 0)
    assert resolve_token(store, TOKEN_SETTING_PREFIX) is None


# --- the throttle: one write a minute per token, not one per request -------------------------


def test_the_stamp_coalesces_to_one_write_a_minute(store, connections):
    """Six requests, one write. A minute later, the seventh.

    Counted at the connection, because that is the claim: not "the stored value is the same" but "no
    connection was opened, so no statement ran, so the single-writer database was not asked".
    """
    token = mint_token(store, label="hermes", org_ids=[ORG])
    context = resolve_token(store, token)

    assert stamp_token_use(store.db_path, token, context, now=1000.0) is True
    assert len(connections) == 1
    for _ in range(5):
        assert stamp_token_use(store.db_path, token, context, now=1010.0) is False
    assert len(connections) == 1, "a throttled stamp still opened a connection"

    assert stamp_token_use(store.db_path, token, context, now=1061.0) is True
    assert len(connections) == 2


def test_the_throttle_is_per_token_not_global(store, connections):
    """One agent's polling must not freeze the record of a different agent.

    With one global "last write", the busiest credential decides what everybody else's activity
    looks like: two devices on one container, and `--list` could report a laptop as unused for a
    week while it was being used every minute.
    """
    laptop = mint_token(store, label="laptop", org_ids=[ORG])
    server = mint_token(store, label="server", org_ids=[ORG])

    assert stamp_token_use(store.db_path, laptop, resolve_token(store, laptop), now=500.0) is True
    assert stamp_token_use(store.db_path, server, resolve_token(store, server), now=501.0) is True
    assert stamp_token_use(store.db_path, laptop, resolve_token(store, laptop), now=502.0) is False
    assert len(connections) == 2
    assert len(use_rows(store)) == 2


def test_the_write_uses_its_own_short_lived_connection(store, connections):
    """Not `Store`, and not left open.

    The `Store` sabotage is the point: if the stamp ever went back to being a method on the store,
    the containment claim in `tests/test_mcp_read_containment.py` would be quietly false again — the
    object the request path holds is a `PRAGMA query_only` view that cannot write this at all. And
    every connection opened here is closed on the way out: a listener that opened one per
    request and kept them would run a container out of file descriptors over a weekend.
    """
    token = mint_token(store, label="hermes", org_ids=[ORG])

    def sabotage(*args, **kwargs):
        raise AssertionError("the stamp must not go through Store")

    store.set_setting = sabotage                                   # type: ignore[method-assign]
    assert use(store, token) is True
    assert len(connections) == 1
    with pytest.raises(sqlite3.ProgrammingError):
        connections[0].execute("SELECT 1")

    reopened = Store(store.db_path)
    try:
        assert list(use_rows(reopened))
    finally:
        reopened.close()


def test_a_locked_database_skips_the_stamp_instead_of_stalling_the_request(store, monkeypatch):
    """The write is on the auth path, so it may cost a timestamp and nothing else.

    `busy_timeout` on this connection is short and bounded for that reason: waiting out a delivery
    write on behalf of a stamp nobody reads for a week would trade a cosmetic gap in `--list` for a
    slow request on a port an agent is polling.
    """
    from papra_suite import mcp_tokens

    token = mint_token(store, label="hermes", org_ids=[ORG])

    def locked(path):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(mcp_tokens, "_connect", locked)
    assert use(store, token) is False


def test_the_throttle_state_cannot_grow_with_the_traffic(store, monkeypatch):
    """Memory is bounded by the number of credentials, not by requests against the port.

    Only a token that resolved is remembered, and resolving is one digest lookup: a million random
    bearers buy a million `None`s and no dict entries. That is the difference between a counter
    and a way to exhaust a container over Tailscale.
    """
    from papra_suite import mcp_tokens

    for index in range(200):
        assert stamp_token_use(store.db_path, f"guessed-{index}", None, now=10.0) is False
    assert mcp_tokens._recent_stamps == {}

    token = mint_token(store, label="pi", org_ids=[ORG])
    assert use(store, token, now=10.0) is True
    assert len(mcp_tokens._recent_stamps) == 1


def test_the_stamp_is_a_wall_clock_timestamp(store):
    """Monotonic arithmetic for the throttle, wall clock for the value.

    The throttle uses `time.monotonic` so an NTP step can neither freeze the record nor release a
    burst — this container lives in Docker Desktop, where the VM clock does both. The stored
    stamp is wall clock, because a human reads it against a calendar.
    """
    token = mint_token(store, label="hermes", org_ids=[ORG])
    assert use(store, token) is True
    stamp = next(iter(use_rows(store).values()))
    parsed = datetime.fromisoformat(stamp)
    assert parsed.tzinfo is not None and parsed.year >= 2024


async def test_the_auth_path_stamps_once_a_minute_not_once_a_request(store, connections):
    """The wiring: `verify_token` is where the one legitimate write happens.

    Four authenticated requests, one connection and one row. A rejected credential must then cost
    neither: the throttle is per token, and "nothing was written for a stranger" is the sentence
    that keeps this off the DoS list. The verifier is handed a `LedgerReader` and reads through it,
    so the only write left on this path is the stamp itself.
    """
    from papra_suite.mcp_server import StoreTokenVerifier
    from papra_suite.readers import LedgerReader

    token = mint_token(store, label="hermes", org_ids=[ORG])
    view = LedgerReader(store.db_path)
    try:
        verifier = StoreTokenVerifier(view)
        for _ in range(4):
            assert await verifier.verify_token(token) is not None
            assert await verifier.verify_token("a-stranger's-token") is None
    finally:
        view.close()

    assert len(connections) == 1, "the auth path wrote more than the throttled stamp"
    assert len(use_rows(store)) == 1
    assert token_counts(store) == (1, 0)




