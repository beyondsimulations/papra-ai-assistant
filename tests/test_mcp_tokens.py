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
    TOKEN_USE_PREFIX,
    TokenContext,
    TokenError,
    add_subparser,
    list_tokens,
    mint_token,
    resolve_token,
    revoke_token,
    run_mcp_token,
    stamp_token_use,
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


# --- a grant that names no bucket (review I4) -----------------------------------------------


def seen_bucket(store, org):
    """A bucket this suite has recorded seeing a document in.

    `documents_org_ids()` is the wider of the two local bucket listings, so it is the one that must
    silence the warning: a bucket with only unledgered documents is still a real bucket.
    """
    store.record_document_seen(papra_org_id=org, papra_document_id=f"{org}-d1", name="d1.pdf",
                               mime="application/pdf", ingested_via="mail")


def test_create_warns_loudly_when_a_granted_id_names_no_bucket(store, capsys):
    """`org-1` when the real id is `org-01` authenticates, prints `scope: buckets`, and reads as an
    empty archive: a confident wrong answer behind a happy exit code, and the command that minted it
    is the last cheap place to catch it.
    """
    code = run_mcp_token(config_for(store), parse(
        ["--create", "--label", "hermes", "--org-id", "org-1"]), store=store)
    captured = capsys.readouterr()

    assert code == 0, "the token is still minted: a brand-new bucket legitimately has no rows yet"
    err = captured.err
    assert "org-1" in err
    assert "no bucket" in err and "typo" in err
    assert "org-1" in captured.out, "the grant is still printed on the token's own line"


def test_create_is_quiet_about_a_bucket_the_archive_has_seen(store, capsys):
    seen_bucket(store, ORG)
    code = run_mcp_token(config_for(store), parse(
        ["--create", "--label", "hermes", "--org-id", ORG]), store=store)
    captured = capsys.readouterr()

    assert code == 0
    assert captured.err == "", captured.err


def test_create_warns_about_only_the_ids_that_name_nothing(store, capsys):
    seen_bucket(store, ORG)
    run_mcp_token(config_for(store), parse(
        ["--create", "--label", "hermes", "--org-id", ORG, "--org-id", "org-01"]), store=store)
    err = capsys.readouterr().err

    assert "org-01" in err
    assert ORG not in err, "naming a bucket that exists alongside a typo would bury the typo"


def test_create_says_nothing_about_an_all_buckets_grant(store, capsys):
    """`--all-buckets` names no id, so there is nothing to have mistyped."""
    run_mcp_token(config_for(store), parse(
        ["--create", "--label", "op", "--all-buckets"]), store=store)

    assert capsys.readouterr().err == ""


def test_list_flags_a_grant_that_names_no_bucket(store, capsys):
    """The same reading, later, for a token somebody else minted.

    The note is a footer rather than a column: the table is tab-separated and its `buckets` field is
    what an operator copies into a client, so a marker in it would read as part of an id.
    """
    mint_token(store, label="hermes", org_ids=["org-01"])
    seen_bucket(store, ORG)
    mint_token(store, label="pi", org_ids=[ORG])

    code = run_mcp_token(config_for(store), parse(["--list"]), store=store)
    out = capsys.readouterr().out

    assert code == 0
    assert "org-01" in out and "hermes" in out
    assert "no bucket" in out
    rows = [line for line in out.splitlines() if line.startswith(("hermes", "pi"))]
    # Five columns since `last used` joined the table: the point of the assertion is unchanged — the
    # unknown-bucket warning is a FOOTER, so no row may gain a sixth field or lose its shape.
    assert len(rows) == 2 and all(len(line.split("\t")) == 5 for line in rows)


def test_list_is_quiet_when_every_grant_names_a_real_bucket(store, capsys):
    seen_bucket(store, ORG)
    mint_token(store, label="hermes", org_ids=[ORG])
    run_mcp_token(config_for(store), parse(["--list"]), store=store)

    assert "no bucket" not in capsys.readouterr().out


# --- a rejected row is a count, never a key -----------------------------------------------


def test_a_rejected_row_is_reported_as_a_count_and_not_as_its_key(store, caplog):
    """The settings key of a token row **is** the token's sha256.

    A row that fails to parse is worth logging — otherwise a hand-edited or half-written row is a
    credential that silently stopped working. What is not worth logging is the key: it goes to
    stderr and into the `log_lines` ring buffer the dashboard serves, and "no credential material in
    the logs" has to be one rule with no exceptions in it, not a rule with a 256-bit footnote.
    """
    digest = "ab" * 32
    store.set_setting(f"{TOKEN_SETTING_PREFIX}{digest}", "{not json")
    store.set_setting(f"{TOKEN_SETTING_PREFIX}{'cd' * 32}", json.dumps({"label": "half a row"}))

    with caplog.at_level(logging.DEBUG):
        assert list_tokens(store) == []

    printed = "\n".join(record.getMessage() for record in caplog.records)
    assert printed, "nothing was logged, so a row silently stopped working"
    assert "2" in printed, "the count is what gets logged"
    # The bare prefix is fine to name — it is a namespace, not a secret. What may not appear is the
    # 64 hex characters that follow it, in any phrasing.
    assert digest not in printed and "cd" * 32 not in printed
    assert f"{TOKEN_SETTING_PREFIX}{digest}" not in printed



# --- the `last used` column ---------------------------------------------------------------

def test_a_used_token_lists_the_moment_it_was_seen(store):
    """`stamp_token_use` has written rows since Task 11 and nothing has ever read them.

    The question this answers is the one an operator actually has when they are about to revoke
    something: is this credential in use? `mcp-token --revoke` is destructive precisely when the
    answer is yes, and until now the only way to find out was an sqlite session.
    """
    token = mint_token(store, label="laptop", org_ids=[ORG])
    stamp_token_use(store.db_path, token, resolve_token(store, token), now=1000.0)

    listed = {info.label: info for info in list_tokens(store)}

    assert listed["laptop"].last_used is not None
    assert listed["laptop"].last_used.endswith("+00:00")


def test_a_token_that_never_authenticated_reads_never(store):
    """`never` is a fact about the record; an empty cell would be a missing one."""
    mint_token(store, label="spare", org_ids=[ORG])

    assert list_tokens(store)[0].last_used is None


def test_a_revoked_token_keeps_its_last_use(store):
    """The row outlives the credential on purpose: "revoked, last used 3 weeks ago" is the audit."""
    token = mint_token(store, label="old-phone", org_ids=[ORG])
    stamp_token_use(store.db_path, token, resolve_token(store, token), now=1000.0)
    revoke_token(store, label="old-phone")

    listed = {info.label: info for info in list_tokens(store)}
    assert listed["old-phone"].revoked_at is not None
    assert listed["old-phone"].last_used is not None


def test_a_stamp_is_matched_by_digest_not_by_label(store):
    """A label is the operator's name and can be re-minted; the digest is what ties the rows.

    Rotating a device under the same label must not inherit the old device's last use — that would
    make a credential nobody has used look like one that is in use, which is the wrong reason not to
    revoke it.
    """
    first = mint_token(store, label="pi", org_ids=[ORG])
    stamp_token_use(store.db_path, first, resolve_token(store, first), now=1000.0)
    revoke_token(store, label="pi")
    mint_token(store, label="pi", org_ids=[ORG])          # the rotation, same label

    listed = [info for info in list_tokens(store) if info.label == "pi"]
    by_state = {info.revoked_at is None: info for info in listed}
    assert by_state[True].last_used is None, "the new token has never authenticated"
    assert by_state[False].last_used is not None, "the revoked one keeps its own stamp"


def test_the_listing_prints_the_column_and_never_a_credential(store, capsys):
    token = mint_token(store, label="laptop", org_ids=[ORG])
    stamp_token_use(store.db_path, token, resolve_token(store, token), now=1000.0)
    mint_token(store, label="spare", org_ids=[ORG])
    capsys.readouterr()

    code = run_mcp_token(config_for(store), parse(["--list"]), store=store)

    out = capsys.readouterr().out
    assert code == 0
    header, *rows = out.strip().splitlines()
    assert "last used" in header
    body = {line.split("\t")[0]: line.split("\t") for line in rows}
    assert body["laptop"][-1] not in ("never", "") and body["laptop"][-1] != "last used"
    assert any(cell == "never" for cell in body["spare"])
    # The promise of the whole command, restated for the new column: no plaintext, and no digest of
    # anything — the stamp rows are keyed by the same sha256 as the token rows, and that key must
    # not become a printable identifier.
    assert token not in out and TOKEN_SETTING_PREFIX not in out
    assert hashlib.sha256(token.encode()).hexdigest() not in out


def test_a_stamp_row_cannot_forge_a_column(store):
    """`mcp_token_use:*` is a settings row, and settings rows can be hand-edited."""
    token = mint_token(store, label="laptop", org_ids=[ORG])
    stamp_token_use(store.db_path, token, resolve_token(store, token), now=1000.0)
    store.set_setting(TOKEN_USE_PREFIX + hashlib.sha256(token.encode()).hexdigest(),
                      "2026-11-19T10:00:00+00:00\tnext\\tand\nmore")

    info = list_tokens(store)[0]

    assert info.last_used is not None
    assert "\n" not in info.last_used and "\t" not in info.last_used
