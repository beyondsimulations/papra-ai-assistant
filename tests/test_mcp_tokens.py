"""Task 12, part 1: the bearer-token store behind `papra-suite mcp-token`.

The token is the only access control on this server — the tailnet is treated as private and no
OAuth is in play — so the store's job is to answer one question and to never answer any other:
*which scope does this plaintext carry?* Everything else about a token is derived from that.

Three properties shape the file:

**The plaintext is never stored.** The settings key is the sha256 of the token, so a database dump,
a backup, or a `SELECT *` in a support ticket cannot be pasted into a client. This is why
`resolve_token` hashes the candidate and looks the key up exactly: there is no scan over stored
tokens to compare against, and therefore no prefix match, no ordering, and no oracle.

**Scoping is a security boundary, not a filter.** A token carries a bucket list or `scope_all`.
Nothing downstream may widen it, and a token with no buckets sees nothing but its own existence.

**Every field is attacker-influenceable text.** A label is chosen by whoever runs the command and
echoed back by `--list`; org ids come from the same keyboard. Both are bounded and screened at
mint, because the alternative is to sanitize them at every print site forever.
"""

import base64
import hashlib
import json
import logging

import pytest

from papra_suite.mcp_tokens import (
    LABEL_MAX_CHARS,
    MAX_ORG_IDS,
    ORG_ID_MAX_CHARS,
    TOKEN_SETTING_PREFIX,
    TokenContext,
    TokenError,
    add_subparser,
    list_tokens,
    mint_token,
    resolve_token,
    revoke_token,
    run_mcp_token,
    token_counts,
)
from papra_suite.store import Store

ORG = "org-1"


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.db_path = tmp_path / "state.db"
    yield store
    store.close()


def all_settings(store):
    """Every (key, value) pair in `settings`, as one haystack."""
    return "\n".join(
        f"{key}={store.get_setting(key)}" for key in store.list_setting_keys())


def rows(store):
    """The raw settings rows whose key starts with the token prefix."""
    return {key: json.loads(store.get_setting(key))
            for key in store.list_setting_keys(prefix=TOKEN_SETTING_PREFIX)}


# --- what a mint leaves behind ---------------------------------------------------------

def test_mint_stores_only_a_hash_and_returns_the_plaintext_once(store):
    token = mint_token(store, label="hermes", org_ids=[ORG])

    assert token not in all_settings(store)
    # The key is a digest, so the key list cannot be a second copy of the secret either.
    assert token not in "".join(store.list_setting_keys())

    context = resolve_token(store, token)
    assert isinstance(context, TokenContext)
    assert (context.label, context.org_ids, context.scope_all) == (
        "hermes", frozenset({ORG}), False)


def test_the_settings_key_is_the_token_sha256_and_carries_the_documented_fields(store):
    token = mint_token(store, label="pi", org_ids=[ORG, "org-2"])
    key = TOKEN_SETTING_PREFIX + hashlib.sha256(token.encode()).hexdigest()
    assert key in store.list_setting_keys()
    row = json.loads(store.get_setting(key))
    assert set(row) == {"label", "org_ids", "scope_all", "created_at", "revoked_at"}
    assert row["revoked_at"] is None
    # The store's own timestamp shape: `+00:00`, not `Z`. `_require_store_utc_iso` says why — these
    # strings are compared as text, and a `Z` suffix sorts after every offset.
    assert row["created_at"].endswith("+00:00")


def test_a_token_carries_at_least_32_bytes_of_entropy(store):
    """`secrets.token_urlsafe(32)` is a floor, not a decoration.

    A 32-character *string* can hold 24 bytes; the spec's minimum is about entropy, so the check is
    on the decoded length. A token that short is guessable at a rate the tailnet does not protect
    against.
    """
    token = mint_token(store, label="pi", org_ids=[ORG])
    padded = token + "=" * (-len(token) % 4)
    assert len(base64.urlsafe_b64decode(padded)) >= 32


def test_two_mints_are_different_tokens(store):
    first = mint_token(store, label="a", org_ids=[ORG])
    second = mint_token(store, label="b", org_ids=[ORG])
    assert first != second


# --- scope ------------------------------------------------------------

def test_two_tokens_do_not_share_scope(store):
    hermes = mint_token(store, label="hermes", org_ids=[ORG, "org-2"])
    pi = mint_token(store, label="pi", org_ids=["org-3"])

    assert resolve_token(store, hermes).org_ids == frozenset({ORG, "org-2"})
    assert resolve_token(store, pi).org_ids == frozenset({"org-3"})


def test_a_scope_all_token_has_no_bucket_list(store):
    token = mint_token(store, label="operator", org_ids=[], scope_all=True)
    context = resolve_token(store, token)
    assert context.scope_all is True and context.org_ids == frozenset()


def test_a_token_with_no_grant_sees_nothing_but_itself(store):
    """The empty scope is a real state, and it must not read as "all buckets"."""
    token = mint_token(store, label="nobody", org_ids=[])
    context = resolve_token(store, token)
    assert context.org_ids == frozenset() and context.scope_all is False


def test_a_duplicate_active_label_is_refused_and_writes_nothing(store):
    mint_token(store, label="pi", org_ids=[ORG])
    before = rows(store)

    with pytest.raises(TokenError, match="already"):
        mint_token(store, label="pi", org_ids=["org-9"])

    assert rows(store) == before


def test_a_revoked_label_can_be_minted_again(store):
    """Rotation: the name is reused, the credential is not.

    Refusing forever would mean every rotation invents `pi-2`, `pi-3`; the row that keeps the old
    hash is marked revoked, so it cannot be confused with the live one.
    """
    first = mint_token(store, label="pi", org_ids=[ORG])
    revoke_token(store, label="pi")
    second = mint_token(store, label="pi", org_ids=["org-2"])

    assert first != second
    assert resolve_token(store, first) is None
    assert resolve_token(store, second).org_ids == frozenset({"org-2"})


def test_scope_all_and_a_bucket_list_together_are_refused(store):
    with pytest.raises(TokenError, match="both"):
        mint_token(store, label="confused", org_ids=[ORG], scope_all=True)
    assert rows(store) == {}


# --- revocation and lookup ------------------------------------------------

def test_revoked_token_resolves_to_nothing(store):
    token = mint_token(store, label="pi", org_ids=[ORG])
    assert revoke_token(store, label="pi") is True
    assert resolve_token(store, token) is None
    assert resolve_token(store, token + "x") is None      # no partial-match acceptance


def test_revoking_twice_reports_the_second_as_nothing_to_do(store):
    mint_token(store, label="pi", org_ids=[ORG])
    assert revoke_token(store, label="pi") is True
    assert revoke_token(store, label="pi") is False


def test_revoking_an_unknown_label_reports_false(store):
    assert revoke_token(store, label="never-minted") is False


@pytest.mark.parametrize("candidate", [
    "", " ", "Bearer not-a-token", "short",
    "0123456789abcdef0123456789abcdef",      # 32 chars of nothing stored here
])
def test_lookup_only_ever_answers_for_a_minted_plaintext(store, candidate):
    mint_token(store, label="pi", org_ids=[ORG])
    assert resolve_token(store, candidate) is None


def test_a_prefix_of_a_token_does_not_resolve(store):
    """A truncated bearer value is what a proxy log or a clipboard slip produces."""
    token = mint_token(store, label="pi", org_ids=[ORG])
    for cut in (1, len(token) // 2, len(token) - 1):
        assert resolve_token(store, token[:cut]) is None


def test_a_malformed_row_fails_closed(store):
    """Settings are a table anyone can edit; a row that is not a token document is refused.

    A half-written or hand-edited row must not become "no scopes" — that would be a token with a
    scope, and a corrupt row that grants the corpus is the failure this store must not have.
    """
    token = mint_token(store, label="pi", org_ids=[ORG])
    key = TOKEN_SETTING_PREFIX + hashlib.sha256(token.encode()).hexdigest()
    store.set_setting(key, "{not json")
    assert resolve_token(store, token) is None

    store.set_setting(key, json.dumps({"label": "pi", "org_ids": [ORG], "scope_all": True,
                                       "created_at": "2026-11-19T00:00:00Z", "revoked_at": None}))
    assert resolve_token(store, token) is None      # scope_all together with buckets

    store.set_setting(key, json.dumps({"label": "pi", "scope_all": True,
                                       "created_at": "2026-11-19T00:00:00Z", "revoked_at": None}))
    assert resolve_token(store, token) is None      # org_ids missing entirely


# --- listing and counting ----------------------------------------------

def test_list_tokens_names_labels_scopes_and_state(store):
    mint_token(store, label="hermes", org_ids=[ORG, "org-2"])
    pi = mint_token(store, label="pi", org_ids=["org-3"], scope_all=False)
    revoke_token(store, label="pi")

    listed = {info.label: info for info in list_tokens(store)}
    assert set(listed) == {"hermes", "pi"}
    assert listed["hermes"].org_ids == (ORG, "org-2")
    assert listed["hermes"].revoked_at is None
    assert listed["pi"].revoked_at is not None
    assert listed["pi"].scope_all is False
    # Nothing on a listed token is a credential: neither plaintext nor the hash it is stored under.
    for info in listed.values():
        assert pi not in repr(info) and TOKEN_SETTING_PREFIX not in repr(info)


def test_list_tokens_sorts_by_label(store):
    for label in ("zeta", "alpha", "mid"):
        mint_token(store, label=label, org_ids=[ORG])
    assert [info.label for info in list_tokens(store)] == ["alpha", "mid", "zeta"]


def test_token_counts_splits_active_from_revoked(store):
    assert token_counts(store) == (0, 0)
    mint_token(store, label="a", org_ids=[ORG])
    mint_token(store, label="b", org_ids=[ORG])
    revoke_token(store, label="b")
    assert token_counts(store) == (1, 1)


def test_a_malformed_row_is_listed_as_a_refused_row_not_a_token(store):
    """A row under the prefix that is not a token document is reported, not served."""
    store.set_setting(TOKEN_SETTING_PREFIX + "deadbeef", "not json")
    mint_token(store, label="pi", org_ids=[ORG])
    assert [info.label for info in list_tokens(store)] == ["pi"]
    assert token_counts(store) == (1, 0)


# --- bounds on the text a token can carry ---------------------------------

def test_a_label_is_bounded_and_screened(store):
    with pytest.raises(TokenError, match="length"):
        mint_token(store, label="x" * (LABEL_MAX_CHARS + 1), org_ids=[ORG])
    for bad in ("has\ttab", "new\nline", "esc\x1b[2J", ""):
        with pytest.raises(TokenError):
            mint_token(store, label=bad, org_ids=[ORG])
    assert rows(store) == {}


def test_org_ids_are_bounded_screened_and_deduplicated(store):
    with pytest.raises(TokenError, match="length"):
        mint_token(store, label="pi", org_ids=["o" * (ORG_ID_MAX_CHARS + 1)])
    with pytest.raises(TokenError):
        mint_token(store, label="pi", org_ids=[" "] * MAX_ORG_IDS)
    with pytest.raises(TokenError, match="8"):
        mint_token(store, label="pi", org_ids=[f"org-{n}" for n in range(MAX_ORG_IDS + 1)])

    token = mint_token(store, label="pi", org_ids=["org-2", ORG, "org-2"])
    assert resolve_token(store, token).org_ids == frozenset({ORG, "org-2"})
    assert list_tokens(store)[0].org_ids == tuple(sorted([ORG, "org-2"]))


# --- the CLI ---------------------------------------------------------------

def parse(argv):
    import argparse

    parser = argparse.ArgumentParser(prog="papra-suite")
    sub = parser.add_subparsers(dest="command", required=True)
    add_subparser(sub)
    return parser.parse_args(["mcp-token", *argv])


def config_for(store):
    """A config pointing at the fixture's data dir, for the paths that open their own store."""
    from types import SimpleNamespace

    return SimpleNamespace(data_dir=store.db_path.parent)


def test_cli_parses_the_documented_surface():
    create = parse(["--create", "--label", "pi", "--org-id", "org-1"])
    assert (create.create, create.label, create.org_id) == (True, "pi", ["org-1"])
    assert parse(["--create", "--label", "pi", "--all-buckets"]).all_buckets is True
    assert parse(["--list"]).list is True
    revoke = parse(["--revoke", "--label", "pi"])
    assert (revoke.revoke, revoke.label) == (True, "pi")


def test_cli_refuses_all_buckets_with_a_bucket_list_at_the_parser():
    with pytest.raises(SystemExit) as caught:
        parse(["--create", "--label", "pi", "--org-id", "org-1", "--all-buckets"])
    assert caught.value.code == 2


def plaintext_from(out, store):
    """The one printed line that is a live token — the CLI's own output, read back."""
    found = [line for line in out.splitlines() if resolve_token(store, line.strip()) is not None]
    assert len(found) == 1, out
    return found[0].strip()


def test_cli_create_prints_the_plaintext_once_between_two_warnings(store, capsys, caplog):
    with caplog.at_level(logging.DEBUG):
        code = run_mcp_token(config_for(store), parse(
            ["--create", "--label", "pi", "--org-id", "org-1"]), store=store)
    out = capsys.readouterr().out
    assert code == 0
    lines = [line for line in out.splitlines() if line]
    token = plaintext_from(out, store)
    assert len(out.split(token)) == 2, "the plaintext must appear exactly once"
    index = lines.index(token)
    assert index == 1 and len(lines) == 3, out
    for warning in (lines[0], lines[2]):
        assert "once" in warning or "cannot" in warning, warning


def test_cli_create_writes_no_log_line_containing_the_token(store, capsys, caplog):
    with caplog.at_level(logging.DEBUG):
        run_mcp_token(config_for(store), parse(
            ["--create", "--label", "pi", "--all-buckets"]), store=store)
    token = plaintext_from(capsys.readouterr().out, store)
    assert token not in capsys.readouterr().err
    for record in caplog.records:
        assert token not in record.getMessage()
        assert token not in str(record.args or "")


def test_cli_create_exits_nonzero_on_a_duplicate_label(store, capsys):
    run_mcp_token(config_for(store), parse(["--create", "--label", "pi", "--all-buckets"]),
                  store=store)
    capsys.readouterr()

    code = run_mcp_token(config_for(store), parse(
        ["--create", "--label", "pi", "--org-id", "org-1"]), store=store)
    err = capsys.readouterr().err
    assert code != 0
    assert "pi" in err and "papra-suite mcp-token --list" in err


def test_cli_list_prints_labels_scopes_and_state_and_no_credential(store, capsys):
    minted = mint_token(store, label="hermes", org_ids=[ORG, "org-2"])
    mint_token(store, label="operator", org_ids=[], scope_all=True)
    revoke_token(store, label="hermes")
    capsys.readouterr()

    code = run_mcp_token(config_for(store), parse(["--list"]), store=store)
    out = capsys.readouterr().out
    assert code == 0
    assert "hermes" in out and "operator" in out
    assert f"{ORG},org-2" in out.replace(" ", "") or f"{ORG}, org-2" in out
    assert "ALL" in out
    assert minted not in out and TOKEN_SETTING_PREFIX not in out
    assert "revoked" in out


def test_cli_list_says_so_when_there_are_no_tokens(store, capsys):
    code = run_mcp_token(config_for(store), parse(["--list"]), store=store)
    assert code == 0
    assert "no tokens" in capsys.readouterr().out


def test_cli_revoke_reports_what_stops_working(store, capsys):
    mint_token(store, label="pi", org_ids=[ORG])
    mint_token(store, label="hermes", org_ids=[ORG])
    capsys.readouterr()

    code = run_mcp_token(config_for(store), parse(["--revoke", "--label", "pi"]), store=store)
    out = capsys.readouterr().out
    assert code == 0
    assert "pi" in out and "1 token" in out and "1 still active" in out


def test_cli_revoke_of_an_unknown_label_exits_nonzero(store, capsys):
    code = run_mcp_token(config_for(store), parse(["--revoke", "--label", "ghost"]), store=store)
    err = capsys.readouterr().err
    assert code != 0 and "ghost" in err


def test_cli_refuses_an_unmigrated_store_on_list(tmp_path, capsys):
    """`--list` reads; reading must not migrate, and it must say what is missing."""
    from papra_suite.config import load_config
    from tests.test_config import BASE_ENV

    config = load_config(dict(BASE_ENV, DATA_DIR=str(tmp_path / "fresh")))
    code = run_mcp_token(config, parse(["--list"]))
    err = capsys.readouterr().err
    assert code == 2 and "check" in err

    probe = Store(config.data_dir / "state.db")
    try:
        assert probe.table_columns("settings") == []      # a read migrated nothing
    finally:
        probe.close()


def test_cli_create_writes_the_row_and_migrates(tmp_path, capsys):
    from papra_suite.config import load_config
    from tests.test_config import BASE_ENV

    config = load_config(dict(BASE_ENV, DATA_DIR=str(tmp_path / "fresh")))
    code = run_mcp_token(config, parse(["--create", "--label", "pi", "--all-buckets"]))
    assert code == 0
    store = Store(config.data_dir / "state.db")
    try:
        token = plaintext_from(capsys.readouterr().out, store)
        assert resolve_token(store, token) is not None
        assert token_counts(store) == (1, 0)
    finally:
        store.close()
