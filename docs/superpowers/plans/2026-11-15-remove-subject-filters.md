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

## Task 3: header-first fetch

The poller must fetch headers before bodies so a rejected sender costs one header read.
This is a new requirement, not existing behaviour — measure the current fetch first.

## Task 4: authentication evidence — blocked, by design

No `Authentication-Results` parsing until the chosen provider is verified to strip or
outrank a sender-supplied header claiming the trusted `authserv-id`. Required test that
must exist and pass: a message forged with its own `dmarc=pass` is **not** ingested.
Until then the verdict is inconclusive and inconclusive is not permission.

## Task 5: diagnostics

`papra-suite check` prints the parsed allow-list, configuration only. A separate
`--diagnose-mail` path uses `BODY.PEEK`, changes no flags or cursors, and prints headers
and verdicts, never message contents.
