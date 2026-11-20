"""`papra-suite check --json`: the same readout, for a script instead of an eye.

The reason this command exists is the corpus backfill. Extraction on the operator's hardware costs
two to three minutes a page, so a full pass is a multi-day job, and "how far along is it?" becomes a
question worth asking on a timer rather than by hand. A terminal line answers it; a shell that wants
to graph it has to parse that line, and parsing prose is how a monitoring script ends up reading a
number out of the wrong clause the day somebody rewords a sentence.

What these tests are therefore built to protect is not the JSON's shape but the *agreement* between
the two renderings of one read: nearly every assertion here builds the expected text line out of the
JSON payload and requires that exact text in the human output. If a future change computes a number
twice and gets it wrong once, that is what fails.
"""
import json
import sys

from tests.test_config import BASE_ENV
from tests.test_main import _seed_index_corpus, _seed_oversized


def _run(monkeypatch, tmp_path, capsys, *extra):
    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "check", *extra])
    from papra_suite.__main__ import main

    code = main()
    return code, capsys.readouterr().out


def _seed_events(tmp_path):
    """Three refusals, two kinds, so `by_kind` has more than one shape to get wrong."""
    from papra_suite.store import Store

    store = Store(tmp_path / "state.db")
    try:
        store.migrate()
        for uid in (7, 8):
            store.record_ingest_event(mailbox_id="mb_1", uid=uid, kind="upload_failed",
                                      papra_org_id="org_1", filename="scan.pdf",
                                      sender="a@x.com", detail="papra said no")
        store.record_ingest_event(mailbox_id="mb_1", uid=9, kind="sender_malformed",
                                  sender="Not a header <",
                                  detail="unparseable From")
    finally:
        store.close()


def _seed_deletion_stamp(tmp_path, stamp="ok 2026-11-19T10:00:00+00:00 walked 4 buckets"):
    """The last delete pass's own verdict, which is a settings row `check` only reads."""
    from papra_suite import indexing
    from papra_suite.store import Store

    store = Store(tmp_path / "state.db")
    try:
        store.migrate()
        store.set_setting(indexing.LAST_DELETION_PASS_SETTING, stamp)
    finally:
        store.close()


def _seed_listener(tmp_path):
    """One live credential and a listener the config would start, so the mcp group has state.

    With the listener off and no tokens, the payload is `{"state": "disabled", 0, 0}` and a payload
    built from anything else that also says "disabled, 0, 0" passes — which is a mutant, not a
    feature. Enabled, with a token, every field in the group has a different value to be wrong
    about.
    """
    from papra_suite import indexing
    from papra_suite.mcp_tokens import mint_token
    from papra_suite.store import Store

    store = Store(tmp_path / "state.db")
    try:
        store.migrate()
        store.set_setting("mcp_server_enabled", "1")
        store.set_setting(indexing.LAST_DELETION_PASS_SETTING,
                          "ok 2026-11-19T10:00:00+00:00 walked 4 buckets")
        mint_token(store, label="hermes", org_ids=["org_1"])
    finally:
        store.close()


def test_check_json_is_one_document_and_no_prose(monkeypatch, tmp_path, capsys):
    """stdout is exactly one JSON value: a pipe to `jq` must not need `tail -1`.

    A header line here would be harmless to a human and would break every script written against
    it, so the assertion is that the whole of stdout parses.
    """
    code, out = _run(monkeypatch, tmp_path, capsys, "--json")

    assert code == 0
    payload = json.loads(out)
    assert isinstance(payload, dict)
    # The keys are the contract a script codes against; a rename is a breaking change and this
    # assertion is what makes it a decision rather than a side effect.
    assert set(payload) == {
        "data_dir", "max_document_bytes", "senders", "events", "index", "index_unavailable",
        "deletions", "oversized", "content_writer", "metadata_writer", "mcp"}


def test_check_json_still_migrates_and_reports_the_data_dir(monkeypatch, tmp_path, capsys):
    """`--json` is the same command, not a lighter one: it migrates, and says where."""
    code, out = _run(monkeypatch, tmp_path, capsys, "--json")

    assert code == 0
    payload = json.loads(out)
    assert payload["data_dir"] == str(tmp_path)
    assert (tmp_path / "state.db").exists()
    assert payload["max_document_bytes"] == 104857600


def test_the_json_index_counts_and_the_text_line_agree(monkeypatch, tmp_path, capsys):
    """One read, two renderings — asserted by rebuilding the sentence out of the payload.

    The fixture is the seeded three-document corpus, whose counts are not all distinct, which is
    exactly why this test reconstructs the whole line from the payload rather than spot-checking a
    field: a payload that swapped `indexed_current` for `repairs_24h` (both 2 here) would still
    print a line that disagrees with the payload in ordering, and this catches it.
    """
    _seed_index_corpus(tmp_path)

    _, text = _run(monkeypatch, tmp_path, capsys)
    code, out = _run(monkeypatch, tmp_path, capsys, "--json")
    assert code == 0
    stats = json.loads(out)["index"]

    assert stats == {
        "references": 3, "indexed_current": 2, "stale": 1, "unledgered": 0, "delivered": 1,
        "extraction_empty": 1, "delivery_blocked": 0, "protected_name": 2, "protected_date": 1,
        "protected_notes": 1, "repairs_24h": 2, "cleanup_pending": 2, "cleanup_failed": 1}
    assert (f"index: {stats['references']} refs, {stats['indexed_current']} current, "
            f"{stats['stale']} stale, {stats['unledgered']} unledgered, "
            f"{stats['delivered']} delivered, {stats['extraction_empty']} extraction-empty, "
            f"{stats['delivery_blocked']} delivery-blocked, "
            f"protected_name={stats['protected_name']} "
            f"protected_date={stats['protected_date']} "
            f"protected_notes={stats['protected_notes']}, "
            f"{stats['repairs_24h']} repairs(24h), {stats['cleanup_pending']} "
            f"cleanup-pending (failed {stats['cleanup_failed']})") in text


def test_the_json_counts_agree_with_the_store_itself(monkeypatch, tmp_path, capsys):
    """The payload is `ledger_stats()`, not a second query that can grow a different opinion."""
    from papra_suite.store import Store

    _seed_index_corpus(tmp_path)
    store = Store(tmp_path / "state.db")
    try:
        direct = store.ledger_stats()
    finally:
        store.close()

    _, _ = _run(monkeypatch, tmp_path, capsys)
    code, out = _run(monkeypatch, tmp_path, capsys, "--json")

    assert code == 0
    assert json.loads(out)["index"] == direct


def test_the_json_oversized_and_writer_and_mcp_fields_agree_with_the_text(
        monkeypatch, tmp_path, capsys):
    """Every remaining group, tied to its line the same way.

    The oversized record is deliberately seeded with a ceiling *different* from the one in force:
    with both at 104857600, printing the record's ceiling where the current one belongs is
    undetectable, and that swap is exactly the kind of mistake this test exists to catch. The
    content flag is turned on for the same reason: with both writer flags off, the two writer lines
    are the
    same sentence, and a payload that swapped the two groups would agree with the terminal forever.
    """
    monkeypatch.setenv("PAPRA_INDEX_CONTENT_DELIVERY", "true")
    # Delivery without turning the legacy worker off is the two-writer arrangement, and `check`
    # refuses it at the door — so the discriminating configuration is delivery on, legacy off.
    monkeypatch.setenv("PAPRA_LEGACY_WORKERS", "false")
    _seed_index_corpus(tmp_path)
    _seed_oversized(tmp_path, limit=524288000, extra=[("doc-scan-2", 209715200)])
    _seed_deletion_stamp(tmp_path)

    _, text = _run(monkeypatch, tmp_path, capsys)
    code, out = _run(monkeypatch, tmp_path, capsys, "--json")
    assert code == 0
    payload = json.loads(out)

    over = payload["oversized"]
    assert over["count"] == 2 and over["largest"] == 209715200
    assert over["limit"] == 524288000 and over["limit_in_force"] == 104857600
    assert f"oversized: {over['count']} seen since {over['since']}" in text
    assert f"largest {over['largest']} bytes" in text
    assert f"limit in force {over['limit_in_force']}" in text
    assert f"refused at {over['limit']}" in text

    assert payload["deletions"]["stamp"] == "ok 2026-11-19T10:00:00+00:00 walked 4 buckets"
    assert f"deletions: {payload['deletions']['stamp']}" in text

    assert payload["senders"]["describe"] in text
    assert f"senders: {payload['senders']['describe']}" in text
    assert f"content writer: {payload['content_writer']['state']}" in text
    assert f"metadata writer: {payload['metadata_writer']['state']}" in text
    assert payload["content_writer"]["delivery"] is True
    assert payload["metadata_writer"]["delivery"] is False
    assert payload["content_writer"]["legacy_workers"] is False
    # The two states must be different words, or the assertion above cannot tell the groups apart.
    assert payload["content_writer"]["state"] != payload["metadata_writer"]["state"]


def test_the_json_mcp_group_agrees_with_the_line_when_the_listener_is_on(
        monkeypatch, tmp_path, capsys):
    """The enabled branch, which the disabled store in every other test cannot reach.

    `mcp: disabled` is what a fresh install prints, so a payload that reported the listener's state
    from thin air would agree with the line in that one case forever. Enabled, with one credential
    and a resolved bind, each field has to come from the store: state, both counts, and the bind the
    line prints.
    """
    monkeypatch.setenv("PAPRA_MCP_URL", "https://papra-mcp.ts.net")
    _seed_listener(tmp_path)

    _, text = _run(monkeypatch, tmp_path, capsys)
    code, out = _run(monkeypatch, tmp_path, capsys, "--json")
    assert code == 0
    mcp = json.loads(out)["mcp"]

    assert mcp["state"] == "enabled"
    assert mcp["tokens_active"] == 1 and mcp["tokens_revoked"] == 0
    assert mcp["bind"] == "127.0.0.1:8765"
    assert mcp["bind_host"] == "127.0.0.1" and mcp["bind_port"] == 8765
    assert f"mcp: enabled on {mcp['bind']}" in text
    assert f"{mcp['tokens_active']} active, {mcp['tokens_revoked']} revoked" in text
    # The label is not in the payload: `check` output gets pasted into issues, and a label names a
    # device. `mcp-token --list` is the command for names.
    assert "hermes" not in out


def test_the_events_group_names_kinds_and_counts_the_line_does(monkeypatch, tmp_path, capsys):
    """An empty table is `none` in the line and `{}` in the payload — then a populated one is both.

    The empty case alone would let a payload that dropped every count pass, so the seeded case is
    the one that matters: each kind and each count has to appear in the line the operator reads.
    """
    _, text = _run(monkeypatch, tmp_path, capsys)
    code, out = _run(monkeypatch, tmp_path, capsys, "--json")

    assert code == 0
    assert json.loads(out)["events"] == {"window_hours": 24, "by_kind": {}}
    assert "events (last 24h): none" in text

    _seed_events(tmp_path)
    _, text = _run(monkeypatch, tmp_path, capsys)
    code, out = _run(monkeypatch, tmp_path, capsys, "--json")

    assert code == 0
    events = json.loads(out)["events"]
    assert events["by_kind"] == {"upload_failed": 2, "sender_malformed": 1}
    assert "events (last 24h): sender_malformed=1, upload_failed=2" in text
    # `--json` must not invent a window the line does not claim: the line says 24 hours, the
    # payload says 24, and a script that assumed a day is not reading a different horizon.
    assert events["window_hours"] == 24


def test_check_json_reports_a_corrupt_generation_as_null_and_still_exits_zero(
        monkeypatch, tmp_path, capsys):
    """The `index: unavailable` case, which is the one case a script must not have to string-match.

    A corrupt `index_generation` means no generation is current, so every generation-scoped count
    would be a guess; the store refuses to produce one, and `check` says so and stays at exit 0. In
    JSON that refusal is `index: null` plus the reason — because a payload that reported all-zero
    counts here would tell a watching script the corpus had been emptied.
    """
    from papra_suite.store import Store

    _seed_index_corpus(tmp_path)
    store = Store(tmp_path / "state.db")
    try:
        store.set_setting("index_generation", "rebuild-2026-11-17")
    finally:
        store.close()

    _, text = _run(monkeypatch, tmp_path, capsys)
    code, out = _run(monkeypatch, tmp_path, capsys, "--json")

    assert code == 0
    payload = json.loads(out)
    assert payload["index"] is None
    assert "index_generation" in payload["index_unavailable"]
    assert "index: unavailable" in text
    # The other lines still render: one unreadable group must not cost the operator the readout
    # they came for, in either format.
    assert payload["oversized"]["count"] == 0
    assert payload["mcp"]["state"] == "disabled"


def test_check_json_carries_no_credential_material(monkeypatch, tmp_path, capsys):
    """`--json` is the output that gets piped into a file, a log, or an agent's context.

    `check` loads the whole config, so it *holds* the Papra key, the LLM key and the admin password.
    Holding them is fine; printing them would send a corpus key into whatever collects the script's
    output, and the whole point of `--json` is that the output travels further than a terminal line.
    The canaries are the values `BASE_ENV` already uses, so this fails if a field is ever added that
    dumps config verbatim.
    """
    code, out = _run(monkeypatch, tmp_path, capsys, "--json")

    assert code == 0
    for secret in (BASE_ENV["PAPRA_API_KEY"], BASE_ENV["LLM_API_KEY"],
                   BASE_ENV["ADMIN_PASSWORD"]):
        assert secret not in out
    assert "api_key" not in out and "password" not in out


def test_check_text_output_is_unchanged_by_the_flag(monkeypatch, tmp_path, capsys):
    """The human lines are what gets pasted into bug reports; `--json` adds a mode, not a reword."""
    _seed_index_corpus(tmp_path)
    _, text = _run(monkeypatch, tmp_path, capsys)

    assert "config ok; store migrated" in text
    assert "index: 3 refs, 2 current" in text
    assert "oversized: none seen" in text
    assert "content writer:" in text and "metadata writer:" in text
    assert "{" not in text
