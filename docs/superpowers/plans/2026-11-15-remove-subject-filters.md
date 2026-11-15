# Remove subject filters — implementation plan

**Spec:** `2026-11-15-ingest-simplification-and-sender-allowlist-design.md` (+ Revision 2).
Branch `simplify-ingest` off `main` (`cd6c474`). Baseline: `373 passed, 9 deselected`, ruff clean.

**Measured inventory** (grep for `subject_filter|subject_matches|KillableMatcher|regex_worker|validate_subject_filter`):

| file | hits | action |
|---|---|---|
| `papra_suite/safematch.py` | 436 lines | delete file |
| `papra_suite/regex_worker.py` | 159 lines | delete file |
| `tests/test_safematch.py` | 649 lines, 25 hits | delete file |
| `papra_suite/mail.py` | 16 | see T1.2 |
| `papra_suite/store.py` | 6 | stop reading/writing `subject_filter`; keep column |
| `papra_suite/pipeline.py` | 3 | drop matcher construction/plumbing |
| `papra_suite/__main__.py` | 1 | drop validator wiring |
| `tests/test_mail_poller.py` | 29 | rewrite filter cases as "PDFs are taken, nothing decides" |
| `tests/test_mail_parsing.py` | 20 | delete `subject_matches` tests |
| `tests/test_serve.py` | 7 | drop matcher fixture |
| `tests/test_mail_session.py` | 4 | drop validator case |
| `tests/test_main.py` | 3 | drop validator case |
| `tests/test_store.py` | 2 | column stays, value ignored |

## Task 1: delete the mechanism

- **1.1** `git rm papra_suite/safematch.py papra_suite/regex_worker.py tests/test_safematch.py`.
- **1.2** `papra_suite/mail.py`: remove the `safematch` import, `SUBJECT_FILTER_TIMEOUT_S`,
  `SUBJECT_VALIDATION_TIMEOUT_S`, `validate_subject_filter`, `subject_matches`,
  `_subject_filter_match`, the `matcher` parameter and its pre-validation call, and the
  match branch so the loop takes every message's PDF attachments.
  **Keep, untouched:** dedup, upload-before-retire, cursor-prefix rule, `\Seen` marking,
  cancellation semantics, 3-strike failure path, attachment size cap, PDF magic-byte check,
  `parse_message`. Removing the filter must not remove the reliability around it.
- **1.3** `store.py`: `subject_filter` becomes write-ignored/read-ignored; the column and
  `Mailbox.subject_filter` stay so existing databases open unchanged.
- **1.4** Rewrite tests by behaviour, not by deletion: a mailbox now ingests every message
  containing a supported attachment. Cases that existed to prove "the filter refused this"
  become "nothing refuses this" and must **fail** if the code still consults a pattern.
- **1.5** Gates: `.venv/bin/ruff check .`; `.venv/bin/pytest -q -m "not integration"`;
  `grep -rn "subject_filter\|KillableMatcher\|regex_worker" papra_suite/ tests/` returns
  only the schema column and comments explaining the removal.
- **1.6** Commit.

## Task 2: sender allow-list, permission layer only

Pure function: parse `PAPRA_ALLOWED_SENDERS` (exact address or bare domain,
case-insensitive, display names and angle brackets stripped, **no plus-address stripping**);
empty list denies everything; identity is the single outer `From:`; `Sender:` is not
consulted. Poller disposition: reject = no body fetch, no upload, no dedup row, mark
`\Seen`, advance cursor, count, **poll does not fail and does not count toward 3 strikes**.
Malformed `From:` is attacker input: skipped and counted, never an infrastructure failure,
never a permanently stuck cursor.

## Task 2 status (after review) — shipped shape, and what is still open

Landed as: `allowlist.py` (pure parse + verdict), `AppConfig.allowed_senders` parsed at
load time, a keyword-only required `allowlist` on `poll_mailbox_once` and `_poll_loop`,
and a deny disposition that retires, advances, counts and never fails the poll.

Four findings from review changed the design rather than annotating it:

* **An unconfigured list pauses; it does not deny message by message.** Fetching and
  marking `\Seen` while the operator is still writing `PAPRA_ALLOWED_SENDERS` would
  consume the inbox one message at a time. `PollResult.ingestion_paused` plus a WARNING
  per poll, no fetch, no retirement, and not a poll failure.
* **Duplicate `From:` fields are ambiguous, never joined.** Joining them let
  `tobi@x.com` plus an empty field collapse into one apparently valid address.
* **RFC 2047 decoding must happen after structural parsing, not before.** With
  decode-first, `From: =?utf-8?Q?a=40example=2Ecom?=` parses as the admitted address
  `a@example.com`. Pinned by a mutant: restoring decode-first fails the test.
* **`Sender:` is not consulted**, and the identity is the outer forwarding message — an
  embedded message from an allowed address cannot borrow admission.

Still open, and not claimed: header-first fetching (Task 3) is **not** implemented, so
refused mail is still downloaded and parsed before being refused. The guarantee today is
"refused mail is never ingested", not "refused mail costs nothing".

Recovery path when the list is corrected: refused messages stay on the server and are
never deleted, even with `delete_after_download=True`; replay is a cursor reset, and the
per-poll summary line names how many were refused. A tested replay command is not built.

## Task 3: header-first fetch

The poller must fetch headers before bodies so a rejected sender costs one header read.
This is a new requirement, not existing behaviour — measure the current fetch first.

## Task 3 status — shipped

`fetch_since(last_seen_uid, admit=None)` asks for `BODY.PEEK[HEADER]` after the size probe
and before the body, and a refusal there is recorded in `FetchBatch.denied` instead of
being downloaded, parsed and then refused. The policy stayed in `allowlist`
(`admit_headers`); the session was handed a question about bytes, not a rule, so it can
still be tested as transport. Callers that pass no `admit` pay no extra round-trip.

Header parsing at the transport boundary is `extract_from_field(raw_bytes)`, which joins
folded continuations and reports a repeated `From:` as ambiguous.

Two things it found, both pinned by tests:

* **The stdlib address parser fabricates addresses.** `getaddresses(["a@x.com more"])`
  returns `a@x.commore` — it deletes the space and glues the words into an address nobody
  wrote, which then passes a "is this a valid address" check over the parsed value. The
  rule is therefore applied to the header's own text: an angle-addr must end the value,
  and without angle brackets the value must be nothing but the address. Legacy
  `Display Name a@x.com` is refused as ambiguous; no current client emits it.
* **A folded continuation that muddies the address is refused, not interpreted.**

Refused mail now costs a size probe and one header fetch — not a 20 MB download and a MIME
parse. It still costs those two round-trips; per-message fetches are what the existing
size-cap logic requires, so batching them is a later optimization, not a claim here.

Mutants killed: removing the header gate (the body-request test fails), dropping
`batch.denied` from the poll loop (counters and the cursor fail).

## Task 3b — open, found in review, NOT yet fixed

1. **An unrecognised FETCH response can lose mail.** Both the header fetch and the
   body fetch treat a response whose literal cannot be matched as "the message
   vanished" and skip it. If a later uid succeeds in the same poll, `last_seen_uid`
   advances past the skipped one and it is never searched again — silent loss, not a
   retry. Skipping is only safe when disappearance is *confirmed*; an unexpected
   response shape (PARTIAL, untagged-only, a literal for a different uid) must abort
   the batch so the cursor cannot pass it, and must never be recorded as a sender
   refusal. Needs a response-shape check that verifies the returned uid/section, plus
   a test that a low skipped uid cannot be bypassed by a higher successful one.
2. **A test does not test its name.** `test_a_refusal_before_a_failed_upload_still_advances_the_cursor`
   uses the ordinary StubClient, so it asserts the success path while claiming to
   assert a failed upload. Needs a failing client and cursor == 1.
3. No tested replay path for refused mail after the operator edits the allow-list.

## Task 4: authentication evidence — blocked, by design

No `Authentication-Results` parsing until the chosen provider is verified to strip or
outrank a sender-supplied header claiming the trusted `authserv-id`. Required test that
must exist and pass: a message forged with its own `dmarc=pass` is **not** ingested.
Until then the verdict is inconclusive and inconclusive is not permission.

## Task 5: diagnostics

`papra-suite check` prints the parsed allow-list, configuration only. A separate
`--diagnose-mail` path uses `BODY.PEEK`, changes no flags or cursors, and prints headers
and verdicts, never message contents.
