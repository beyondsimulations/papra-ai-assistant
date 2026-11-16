# Ingest visibility — implementation plan

**Spec:** `docs/superpowers/specs/2026-11-15-ingest-simplification-and-sender-allowlist-design.md`
(+ Revision 2) and the agreed items recorded in `docs/superpowers/HANDOFF.md`
(*Decisions that are settled*, last bullet).

Branch `ingest-visibility` off `main`. Baseline: `401 passed, 11 deselected`, `ruff check .` clean.

Small by design. Four outcomes the operator currently cannot see, one retention rule, one prompt
edit, one pin. No dashboard (M4), no backup system.

## What "visibility" means here, and what it does not

Today a document that never arrives leaves only a log line, and logs rotate. The operator's
question is always the same one — *"why is my invoice missing?"* — and it currently has four
answers, all invisible in the database:

| outcome | today | after |
|---|---|---|
| attachment is not a supported document | `PollResult.attachments_skipped` counter, then gone | one `ingest_events` row |
| sender refused by the allow-list | `PollResult.senders_denied`, then gone | one row per message |
| sender header unresolvable | `PollResult.malformed_senders`, then gone | one row per message |
| message over `MAX_MESSAGE_BYTES` | `PollResult.oversized_skipped`, then gone | one row per message |
| upload failed (non-409) | `logger.error`, then gone | one row per attachment |

Rows are **evidence, not judgement**: what the poller decided and when. The counters in
`PollResult` stay exactly as they are — this adds a durable record, it does not replace the
per-poll summary, and nothing may read the events table to decide whether to ingest.

Out of scope, deliberately: any backup/restore system (the operator's decision is a README
recovery section), the dashboard UI, the `ingest_events` → M4 dashboard wiring, and anything
about content writes (see the write race in the characterisation spec's addendum; that needs its
own spec and is not visibility work).

## Task 1: the table

- **1.1** `papra_suite/store.py`: add `_migration_002_ingest_events` to the module-level
  `MIGRATIONS` list (append only — `_migration_001_initial` is immutable, existing databases open
  unchanged and get bumped from `user_version` 1 to 2).

```sql
CREATE TABLE ingest_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    mailbox_id TEXT NOT NULL,
    uid INTEGER NOT NULL,
    uidvalidity TEXT,
    papra_org_id TEXT,
    kind TEXT NOT NULL,
    filename TEXT,
    sender TEXT,
    detail TEXT
);
CREATE INDEX idx_ingest_events_ts ON ingest_events(ts);
CREATE INDEX idx_ingest_events_mailbox ON ingest_events(mailbox_id, ts);
```

  **`uidvalidity` is stored next to every `uid`, and that is not decoration.** An IMAP UID is only
  meaningful inside one UIDVALIDITY epoch; after a mailbox is rebuilt (server migration, a
  re-created folder) UIDs restart and can name a different message. An event row quoting a bare
  uid is then a pointer at the wrong message, and the recovery advice in Task 6 tells the operator
  to act on that uid. `select_inbox` already returns the value and the poller already holds it, so
  this is free to record and impossible to recover later.
- **1.2** `kind` is a closed set, defined once as a module constant so a typo cannot invent a
  sixth value at a call site:
  `attachment_unsupported`, `sender_denied`, `sender_malformed`, `message_oversized`,
  `upload_failed`.
  `papra_org_id` is nullable because a refusal is decided before the org matters, and a mailbox's
  org can be edited later. `filename` is nullable because two kinds have no attachment and two
  never had a body fetched. `sender` is nullable because the oversized path refuses on size
  before it reads a header. `uid` is `NOT NULL`: every row in this table is about one IMAP message
  and a writer that cannot name one is a different design, not an accident to accommodate.
- **1.3** A row is a **disposition, not an attempt**: one row per message-or-attachment per poll
  that decided about it. A `upload_failed` message stays un-retired and is re-fetched next poll,
  so a persistent failure produces one row per poll — which is the signal that it is persistent,
  and the reason there is no dedup on this table. A message that is refused twice by two different
  rules produces two rows, and the newest one is what the operator sees first.
- **1.4** Two store methods, nothing more:

```python
def record_ingest_event(self, *, mailbox_id, uid, kind, uidvalidity=None,
                        papra_org_id=None, filename=None, sender=None,
                        detail=None) -> None
def list_ingest_events(self, *, mailbox_id=None, kinds=None, since=None,
                       limit=200) -> list[IngestEvent]
```

  Parameterised SQL only — these strings are attacker-supplied and must never be concatenated
  into a statement. `list_ingest_events` orders `ts DESC, id DESC` (newest first, stable for two
  rows in the same millisecond) and clamps `limit` to 1..1000 rather than trusting the caller.
- **1.5** `IngestEvent` dataclass alongside the others, built by a `_row_to_ingest_event`.
- **1.6** Tests: the migration runs on a database that already has migration 001 applied (open it,
  `migrate()`, assert the table and both indexes exist and `user_version == 2`); migrating twice
  is a no-op; `record` then `list` round-trips every kind including `uidvalidity` and `sender`;
  a row without a uid is rejected by the schema, not silently accepted; `limit` clamping; the
  `id DESC` tie-break.
- **1.7** One cross-feature note that belongs in the commit message: this migration moves
  `user_version` to 2, and `2026-11-16-mcp-search-server.md` Task 7.4 accepts the **range**
  `1..len(MIGRATIONS)` rather than an exact version, specifically so the two features cannot lock
  each other out whichever order they land in.
- **Commit:** `feat(store): ingest_events table for skipped and refused mail`.

## Task 2: untrusted strings are data

Every column value here comes from an attacker-controllable message: `filename` from a MIME
part, `detail` from an exception message that can contain upstream text.

- **2.1** One helper, `sanitize_event_text(value, *, limit)`, used at **every** call site before
  the row is written: coerce to `str`, strip control characters (`ord(ch) < 32 or ch == 127`,
  the same screen `config.py` uses for env values), collapse runs of whitespace, truncate to the
  limit, and return `None` for empty. Limits: filename 200, sender 254, detail 300.
  Truncation appends `…` and the total stays within the limit — a truncation that overflows its
  own budget is the kind of bug this helper exists to prevent.
  Do **not** HTML-escape and do **not** strip quote characters: this is SQLite text bound by
  parameter, and mangling the bytes destroys the evidence the row exists to preserve. Whatever
  renders these rows in M4 escapes at render time, and that is written in the docstring so the
  next person does not "fix" it here.
- **2.2** The `upload_failed` detail is `type(error).__name__` plus `error.status` — **not**
  `str(error)`. `PapraError.__str__` embeds `response.text[:200]`, which is upstream body text and
  can contain anything a server chose to send, including a forged-looking line. The full message
  keeps going to `logger.error` as it does today (the log is where an operator debugges from);
  the row keeps the classification.
- **2.3** Tests: a filename of 5 000 characters, one with `\x00`/`\x1b`/`\r\n`, one that is
  `Robert"; DROP TABLE ingest_events; --`; a `PapraError` whose message carries a fake body — each
  asserts the stored value's length, its control-character freedom, and that the table still
  exists afterwards. Also a test that `str(error)` text does **not** land in `detail`.
- **Commit:** `feat(store): bounded sanitisation for attacker-supplied ingest event fields`.

## Task 3: write the five events from the poller

In `papra_suite/mail.py`, in `_poll_mailbox_once`. Each write goes through one local helper
`_record_event_quietly(store, ...)` modelled on the existing `_record_success_quietly`: it
catches `(ValueError, sqlite3.Error)` and logs. **The poller's never-raises guarantee outranks
the audit trail** — a full disk or a locked database must not turn a successful poll into a
failure, and must not lose the cursor advance.

The helper takes `mailbox_id`, `uid`, `kind` and defaults the rest, and it passes the
`uidvalidity` local that `_poll_mailbox_once` already holds (`select_inbox` returns it at the top
of every poll) into every row, so no call site has to remember it and none can forget it.

- **3.1** `attachment_unsupported` — the existing `for name in parsed.skipped_attachments` loop,
  which already knows the name and the message.
- **3.2** `message_oversized` — the `raw is None` branch, next to the counter it already
  increments.
- **3.3** `sender_denied` / `sender_malformed` — inside `retire_refused`, which already branches
  on the verdict. Both are decisions *not* to ingest, so both are rows; `verdict` maps to `kind`
  and nothing else.
- **3.3b** **Record the refused sender, or the row answers nothing.** Today a header-stage refusal
  carries only `(uid, verdict)` from `MailboxSession.fetch_since` through `FetchBatch.denied`, and
  the address is real evidence the operator needs ("*which* sender did I forget to allow?"). The
  session has the bytes in hand at the moment it refuses: extract the address there with a small
  local helper (`email.message_from_bytes(header).get("From")` through the existing
  `allowlist.parse_from_header`), widen `FetchBatch.denied` to
  `list[tuple[int, str, str | None]]`, and thread it into the row. It is attacker-supplied, so it
  goes through Task 2's helper. The body-stage refusal in `_poll_mailbox_once` already has
  `parsed.from_header` — use the same sanitiser, do not write a second path.
  Callers that construct `FetchBatch` by hand (tests, `test_mail_poller`, `test_mail_session`) get
  updated to the 3-tuple in the same commit; a 2-tuple left behind is a `ValueError` at unpacking,
  which is loud enough.
- **3.4** `upload_failed` — the `except PapraError` arm that sets `uploads_ok = False`. The 409
  arm is **not** an event: "already in Papra" is a success, and recording it would make a
  re-forwarded invoice look like a problem.
- **3.5** `allowlist.paused` writes **no rows**, and the reason goes in a comment: while the gate
  is half-configured nothing is decided about any message, and writing one row per poll per
  mailbox would fill the retention window with the same state and bury the events that answer
  questions. The paused state stays where it is — the `PollResult.ingestion_paused` flag, the log
  line, and `check`'s `senders:` output.
- **3.6** Events are written for the same messages the counters count, in the same loop. A row is
  written **before** the `\Seen` mark is attempted, since a refused message that we then fail to
  retire is still a message we decided about; and a mid-batch exception must not leave rows for
  messages after the failure — the existing freeze-on-failure ordering is the model.
- **3.7** Tests, extending `tests/test_mail_poller.py` with the existing fixture style. Each of
  the five kinds, asserted as a row **and** as the pre-existing counter, so the two cannot drift
  apart; the sender rows additionally assert the refused address landed in `sender` and the
  poll's `uidvalidity` landed in `uidvalidity`. Then the failure-mode tests: a store whose
  `record_ingest_event` raises → the poll still succeeds, still advances the cursor, still marks
  `\Seen`, and logs; a paused allow-list → **zero rows**; a 409 → zero rows and a dedup hash as
  today.
- **3.8** One test per kind that mutates the code to drop the event write and confirms the test
  fails. Counters alone will not catch a missing row, which is precisely how this class of bug
  hides.
- **Commit:** `feat(mail): record why a message or attachment was not ingested`.

## Task 4: 90-day retention

- **4.1** `prune_ingest_events(retention_days: int = 90) -> int` in `store.py`, deleting
  `WHERE ts < ?` against a cutoff built by the module's existing `_iso()` so the string format
  matches what `_now()` wrote (both are UTC ISO with `+00:00`, so lexical comparison is correct —
  and if `_now`'s format ever changes, this is where it breaks, so say so in the docstring).
- **4.2** Called from `pipeline.serve` once at startup and once per sweep pass — not on a new
  timer, not per poll. Sweep is already the periodic heartbeat, so retention adds no concurrency
  and no new failure surface. `INGEST_EVENT_RETENTION_DAYS = 90` as a module constant.
- **4.3** Pruning must never break ingestion: wrap in the same quiet-catch pattern, log at
  warning with the count.
- **4.4** Tests: an event aged past 90 days disappears and one at 89 days survives (build the
  timestamps explicitly, do not sleep); the prune runs on a `serve` startup pass; a store whose
  prune raises does not stop the sweep or the pollers.
- **4.5** Note in `README.md`, one paragraph: events are a 90-day window, not an archive; a
  question about last quarter is answered by logs or by `replay`, not by this table.
- **Commit:** `feat(store): 90-day retention for ingest events`.

## Task 5: reading them before the dashboard exists

- **5.1** `papra-suite events` subcommand in `__main__.py`: `--mailbox-id`, `--kind`, `--since`
  (`YYYY-MM-DD`), `--limit`, `--dry-run`-free and read-only. It opens the **same writable**
  `Store` as `check` does — this command runs on the host, not in the MCP process — but writes
  nothing.
- **5.2** Output is one line per event, newest first, printing only columns that exist, and with
  the untrusted fields quoted so a filename or sender containing spaces or a newline cannot fake a
  second row:

```
2026-11-16T13:04:11+00:00  sender_denied      mailbox=…3f2a uid=18413 uv=8813 sender=<spam@junk.example>
2026-11-16T13:04:11+00:00  upload_failed      mailbox=…3f2a uid=18416 file="Rechnung 11.pdf" detail=PapraError(status=502)
```

  `--json` for scripting, emitting exactly the dataclass fields. Absent values print as `-`, never
  as `None`, so the output is scannable.
- **5.2b** The header-stage refusals are the reason `sender` exists; if Task 3.3b is deferred, this
  column prints `-` and the row is much less useful. Do not print a `from=` field that the schema
  does not have — an example that lies about the data is worse than a narrower one.
- **5.3** **A non-zero exit code when there are no events would be a trap**: this command answers
  "what did the poller refuse?", and "nothing" is a normal answer, not a failure. Exit 0 for an
  empty window; exit 2 only for a bad `--kind`/`--since` or a missing database, in the existing
  `print(..., file=sys.stderr); return 2` style.
- **5.4** `papra-suite check` gains one line: the count of events in the last 24 h per kind.
  That is the line that makes "the mailbox is polling but nothing arrives" a one-command answer.
  It must not fail `check` when the table is empty or the DB is fresh.
- **5.5** Tests: an empty store prints the empty marker and exits 0; a store with one of each kind
  prints five lines and `--kind` filters; a filename containing a newline yields exactly one
  output line; bad `--kind` exits 2; `check` on a fresh database still prints `config ok`.
- **5.6** **A missing database must be refused before the store is opened.** `Store.__init__`
  creates the parent directory *and* the file, so by the time a `Store` exists there is never a
  "missing database" to report. `events` therefore checks `(config.data_dir / "state.db").exists()`
  first and prints the "run `papra-suite check` first" message with exit 2, exactly like the MCP
  startup does for the same condition. A test asserts both that the message appears and that the
  command did not leave a zero-byte database behind.
- **Commit:** `feat(cli): events command and per-kind counts in check`.

## Task 6: README recovery section (this replaces a backup system)

No backup tooling — the operator's decision. What replaces it is a section that answers
"the database is gone, what now?" honestly, including the part that does not fully work.

- **6.1** `README.md`, new section **Recovery**, three cases:
  - **`state.db` lost or corrupt.** Everything in it is lost, and the honest list is: orgs,
    mailboxes and their cursors, the dedup hashes, the `documents` seen-table, the job queue
    (pending *and* failed OCR/title work), the `settings` table — which once MCP exists holds the
    bearer-token hashes and the org allow-list — and the dashboard's recent log lines. What is
    **not** lost: the documents themselves, which live in Papra. Consequences stated plainly: the
    next poll re-reads from UID 0 and re-downloads whatever is still in the inbox; Papra's own
    content dedup answers 409, which this pipeline treats as already-ingested success rather than
    an error, so re-ingest is safe for the documents; and re-ingest does **not** restore jobs for
    documents that were already in Papra, because the sweep, not the poll, covers those.
    So the path is "re-add the orgs and mailboxes, let the sweep run", not "restore from backup" —
    and the operator's MCP tokens are gone, which means every client needs reconfiguring. Say that
    out loud; it is the surprise in this scenario.
  - **A sender was wrongly refused.** `papra-suite events --kind sender_denied` to see what was
    refused, when and by whom; `papra-suite replay --mailbox-id … --from-uid …` to rewind after
    correcting `PAPRA_ALLOWED_SENDERS`; and the note that the uid printed by `events` is only
    meaningful under the `uidvalidity` printed beside it.
  - **OCR/title jobs stuck.** `serve` recovers running jobs at startup; the jobs table is where to
    look, and `events` shows whether the document ever arrived.
- **6.2** Do not write "back up `state.db` periodically" as if it were a strategy. Either the
  copy is restorable or the sentence is decoration; the section says what a restore does and does
  not recover.
- **6.3** Say explicitly, once, in the same section: **a content write to Papra is not known to be
  durable** — Papra's own extraction can replace it (characterisation spec, addendum). Recovery
  advice that promises "documents come back with their OCR text" would be a lie right now.
- **Commit:** `docs(readme): recovery section replacing a backup system`.

## Task 7: two one-line fixes that were agreed and never applied

- **7.1** `papra_suite/titles.py`: drop the trailing ` (German or English)` from
  `TITLE_SYSTEM_PROMPT`. Product scope is documents generally — contracts, personal papers,
  school letters — and naming two languages tells the model to expect two. "in the document's
  language" stays; it is the whole instruction. Update `tests/test_titles.py` to assert the prompt
  contains neither language name, so a re-add fails.
- **7.2** `docker-compose.test.yml`: pin `ghcr.io/papra-hq/papra` by digest
  (`@sha256:d1f596a3…`, the digest every measurement in the specs was taken against) instead of
  `:latest`. `:latest` carries no version signal — pulling today returned the same digest — so an
  unpinned test stack means tomorrow's failure may be an image change and not our code. Leave a
  comment naming the date and that integration expectations are pinned to it.
- **7.3** Run the integration suite after the pin (`PAPRA_REQUIRE_STACK=1`), not just the unit
  gates: changing the image reference is exactly the change that cannot be validated at unit
  level.
- **Commit:** `fix(titles): no language assumptions; test: pin the Papra image by digest`.

## Task 8: review gate

Advisor review of the whole branch, then a pass over:

1. Any path where an event write can fail the poll or stall a cursor.
2. Any event string that reaches a log line or terminal output unbounded or unquoted.
3. Every one of the five kinds has a test that fails when the write is removed.
4. Retention cannot eat a row the operator is currently asking about.
5. `git diff main --stat` shows nothing under `papra_suite/mcp/` and nothing in `ocr.py`,
   `mail.py`'s cursor logic, or `papra_client.py` beyond the error-class change in 2.2.

## Stop conditions

- If recording an event needs a schema change to the mailboxes or jobs tables, stop: the events
  table is additive and if it is not, the design is wrong.
- If `retire_refused` cannot produce a row without reordering the cursor-prefix logic, stop and
  report. The cursor semantics beat the audit trail, always.
- If the digest-pinned image fails an integration test that `:latest` passes, that is a finding
  about a drift we have been living with — report it, do not re-pin to whatever is newest.

---

## Execution log — 2026-11-16, branch `ingest-visibility`

Tasks 1–8 implemented. Final gates: `ruff check .` clean; `pytest -q -m "not
integration"` → 445 passed, 14 deselected; `PAPRA_REQUIRE_STACK=1 pytest -q
tests/integration` → 14 passed. The integration run counts as pinned-image evidence
because the running containers' image IDs are exactly the digests now in
`docker-compose.test.yml` (`papra d1f596a3…`, `greenmail 309ba544…`).

Mutation check for 3.8: deleting each `_record_event_quietly` call site fails tests,
4 of 4 (`sender_denied` and `sender_malformed` share a site; 3 tests fail there).

Where the code differs from the text above, and why:

- **2.1 — sanitiser placement.** Applied inside `record_ingest_event` rather than at
  every call site, so no writer can omit it. Per-field budgets, with a test pinning
  `filename` and `detail` apart.
- **3.x — quiet-catch scope.** `(ValueError, sqlite3.Error)`, as this plan wrote. Letting
  the store's `ValueError` escape would have traded a typo in our own source for a
  frozen cursor and, three polls later, a disabled mailbox. The closed set is still
  enforced: the store refuses an unknown kind, and every kind has a test asserting its
  **row** appears, so a typo fails in the suite instead of reaching an operator.
- **4.2 — prune points.** Startup and after each sweep pass, as written; the call sits
  outside the sweep loop's `try`, because a prune failure must not swallow the sweep's
  own summary line — that line is the evidence that externally uploaded documents were
  discovered.
- **5.1/5.2 — CLI.** `--since` takes `YYYY-MM-DD` (a day count compared lexically
  against ISO timestamps silently means "the year 7"), and `--json` emits the record
  fields. A missing database is refused before `Store()` exists, per 5.6.
- **7.1 — scope.** `titles.py` changed here; the OCR prompt's identical assumption moved
  to its own branch (`ocr-prompt-language`, based on the shared parent), because gate 5
  excludes `ocr.py` and the diacritic argument deserves a change of its own.

Found while executing, not in the plan:

- **`replay --from-uid N` re-reads strictly after N.** So the uid printed beside a
  refused message has to be passed **minus one** — passing it verbatim reports a
  successful rewind while leaving the one message the operator came for behind.
  Documented in the README and in the option's own help; pinned end-to-end against the
  real IMAP server (replay at N re-reads nothing, at N−1 the document arrives).
- **A quoting rule keyed on whitespace does not stop a terminal.** `ESC` contains no
  whitespace; the renderer now keys on `isprintable()` plus the unsafe Unicode
  categories, and escapes C1 and U+2028/U+2029 in JSON output too. Tested by writing
  such rows straight into the table, which is the case it exists for (older builds and
  hand-edited databases).
- **Two claims in the first README draft were false.** "A message that ingested
  successfully never appears here" — my own integration test forwards a mail whose PDF
  uploaded *and* whose `.zip` was skipped; and "events says whether the document ever
  arrived" — absence here is equally consistent with success, retention, or a failed
  event write. Both rewritten.

Review gate:

1. No path where an event write fails a poll or stalls a cursor: asserted for both
   failure modes (locked database, rejected kind) with upload, `\Seen` and cursor
   advance all still checked.
2. Event strings reach a terminal bounded (store) and escaped (renderer); log lines
   carry our own identifiers, and `upload_failed` stores `PapraError(status=500)`, never
   `str(error)`.
3. Every kind fails a test when its write is removed (4/4 mutants caught).
4. Retention cannot eat a live question: an 89-day row survives, a 91-day row goes, and
   `events` reads without pruning.
5. `git diff main -- papra_suite` touches `__main__.py`, `mail.py`, `pipeline.py`,
   `store.py`, `titles.py` — no `mcp/`, no `ocr.py`, no `papra_client.py`, and the only
   cursor-adjacent diff in `mail.py` is comment text.

Stop conditions: none triggered. `ingest_events` needed no change to `mailboxes` or
`jobs`; `retire_refused` gained its row without touching the cursor-prefix ordering; the
digest-pinned image passed everything `:latest` passed.
