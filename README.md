# papra-suite

A small service that watches one IMAP mailbox, uploads the PDF attachments it
finds to a [Papra](https://github.com/papra-ai/papra) organisation, and asks a
local OpenAI-compatible model to OCR them and name them. It is a companion
service, not a fork: Papra stays the document store.

## How it is meant to be used

One mailbox that exists only for documents — a second Posteo account, or
anything similar. You forward an invoice to it; the invoice lands in Papra.
**The human is the filter**, so there is no rule language to learn and no
pattern to get wrong.

Everything else follows from that choice:

- **Only the sender decides.** `PAPRA_ALLOWED_SENDERS` lists the addresses
  allowed to put things into Papra. A message from anyone else is refused on its
  headers, before its body is downloaded.
- **Only PDFs.** Other attachments are ignored, quietly, as intended.
- **Nothing is deleted unless you ask.** Refused, skipped and failed messages all
  stay on the server, which is what makes recovery possible.

## Setup

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
papra-suite check        # validates config, creates/migrates the SQLite store
papra-suite serve
```

| Variable | Notes |
|---|---|
| `PAPRA_BASE_URL`, `PAPRA_API_KEY` | the Papra instance, e.g. `http://papra:1221` |
| `LLM_BASE_URL`, `LLM_API_KEY` | OpenAI-compatible endpoint (OCR + titles) |
| `TITLE_MODEL`, `OCR_MODEL` | model names on that endpoint |
| `ADMIN_PASSWORD` | dashboard login |
| `PAPRA_ALLOWED_SENDERS` | `me@example.com, family.example.com` — see below |
| `DATA_DIR` | defaults to `/app/data`; the SQLite file lives here |
| `DASHBOARD_BIND` | defaults to `127.0.0.1:8080`; never publish it to the LAN |
| `OCR_MAX_PAGES`, `OCR_PAGE_PIXEL_BUDGET`, `OCR_DPI`, `OCR_CONCURRENCY`, `SWEEP_INTERVAL_S` | optional tuning |

Mailboxes (IMAP host, credentials, poll interval, delete-after-download) are
stored in the database, not in the environment.

### The allow-list, and what it does when it is empty

`PAPRA_ALLOWED_SENDERS=tobi@example.com, family.example.com`

An entry with `@` is one address; without it, a whole domain. Case and display
names are ignored, `+suffixes` are **not** stripped, and an entry that is neither
of those shapes is a startup error rather than a silent no-op.

**No usable entry means paused, not open.** Nothing is fetched, nothing is marked
read, nothing is retired, and `check` says so:

```
senders: allow-list is EMPTY: this deployment denies every sender (...)
```

That is deliberate: the safe failure for a missing variable is a quiet inbox you
notice immediately, not an open door nobody notices.

**What it does not do:** `From:` is a header the sender types. This filters mail
*claiming* an admitted identity. Real spoofing needs the provider's own
authentication verdict, and that check is deliberately not shipped until its
trust boundary is verified for a specific provider.

## Commands

| Command | |
|---|---|
| `papra-suite check` | config + migrations, prints the parsed allow-list, changes nothing |
| `papra-suite serve` | pollers, OCR and title workers, sweep loop |
| `papra-suite replay --mailbox-id ID [--from-uid N] [--dry-run]` | rewind the poll cursor |
| `papra-suite events [--limit N] [--mailbox-id ID] [--kind K] [--since D]` | why messages and attachments were **not** ingested |

`replay` is the answer to "I set the allow-list wrong for a week." Refused mail
was retired but never deleted, so rewinding re-reads it, and Papra's content-hash
dedup means already-ingested documents are not uploaded twice. It also
re-enables a mailbox that repeated failures had disabled, and refuses to move the
cursor forward.

### Where did my attachment go?

`papra-suite events` is the durable answer. The per-poll log lines and the summary
counters say *how many*; this says *which one, from whom, and why*, and it survives
log rotation. Output is one event per line, tab-separated, newest first, and prints
from whatever columns the store actually has.

| `kind` | what happened | what the row can tell you |
|---|---|---|
| `sender_denied` | the allow-list refused the sender | the `From:` text that was refused |
| `sender_malformed` | `From:` did not resolve to exactly one sender | the header text that could not be read |
| `attachment_unsupported` | an attachment that is not a supported document | its filename, and who sent it |
| `message_oversized` | the message is over the per-message size cap | the uid only: the body was never read |
| `upload_failed` | Papra refused the upload | filename and `PapraError(status=…)`, never the response body |

Every row carries `uid` and `uidvalidity`. That pair is what makes it actionable:
the message is still on the server, and the two values together name it — a bare uid
does not, because a rebuilt mailbox restarts its numbering and can point at a
different message. `uid` is also the value `replay --from-uid` wants.

Two things this table is not:

- **It is not an archive.** Rows are kept for 90 days and pruned at every `serve`
  startup, so an empty result can mean "outside the window". It names its own
  window when it prints nothing, and it restates the filter you asked with.
- **It is not a delivery log.** A message that ingested successfully never appears
  here, and a persistent upload failure writes one row per poll for the same uid.
  Count the rows per uid before concluding there were five bad messages.

A refused sender is a normal outcome, not a fault: the mail is marked `\Seen`,
never deleted, and the cursor moves past it. So the recovery for "the allow-list
was wrong" is to fix `PAPRA_ALLOWED_SENDERS` and run `replay` — the mail is still
there.

## When something goes wrong

- **Nothing arrives, no errors** → run `check`. An empty allow-list pauses.
- **A mailbox turned red** → three consecutive poll failures disabled it. The
  stored error says why; `replay` clears the state and re-enables it.
- **A specific message is missing** → `papra-suite events`. It says which uid was
  refused, skipped or failed and why; `replay` re-reads it once the cause is fixed.
- **An attachment vanished** → `papra-suite events --kind attachment_unsupported`.
  This is the only durable place a skipped attachment appears: the message it came
  in on is marked `\Seen` and, with delete-after-download, deleted, so there is
  nothing left to go and look at.

### What losing `state.db` costs

The SQLite file under `DATA_DIR` is the only state this software keeps. Papra holds
the documents; everything about *how* they got there lives in that one file:

| Lost with `state.db` | Consequence |
|---|---|
| Organisations and mailboxes (host, user, **password**) | re-add every mailbox; the IMAP passwords are gone, so this is also the file to treat as a secret when backing it up |
| Poll cursors and `UIDVALIDITY` | every mailbox is read from the beginning again |
| Content dedup hashes and the seen-document table | everything is attempted again |
| Job queue (OCR and title work) | pending and in-flight work is gone, and the seen-document table that tells the sweep what it already handled is gone with it — so the sweep re-enqueues OCR for the whole archive and pays the LLM cost again |
| Settings | today that is only the sweep's resume offsets (`sweep_offset:<org>`); losing them re-scans from page one. Anything a later version stores here is lost the same way |
| `log_lines` ring buffer | the in-app log history |
| `ingest_events` | the 90-day "why was this skipped" window |

**The documents themselves are safe in Papra**, and re-ingest is safe: Papra dedups
by content hash per organisation, so a re-uploaded document answers `409
already_exists`, which the pipeline treats as already-ingested success rather than
as an error or a duplicate. So a restore from backup, or simply letting the pollers
run over the whole inbox again, converges instead of duplicating.

What is not recoverable from anywhere else: the mailbox credentials, and the cursor
positions that keep a large inbox from being re-read from the start.

## Development

```bash
make lint          # ruff
make test-unit     # unit tests only
make test          # full suite; needs Docker (compose stack + PAPRA_REQUIRE_STACK=1)
```

Integration tests run a real IMAP server, Papra and an LLM stub; they are
deselected unless `PAPRA_REQUIRE_STACK=1` is set.
