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

`replay` is the answer to "I set the allow-list wrong for a week." Refused mail
was retired but never deleted, so rewinding re-reads it, and Papra's content-hash
dedup means already-ingested documents are not uploaded twice. It also
re-enables a mailbox that repeated failures had disabled, and refuses to move the
cursor forward.

## When something goes wrong

- **Nothing arrives, no errors** → run `check`. An empty allow-list pauses.
- **A mailbox turned red** → three consecutive poll failures disabled it. The
  stored error says why; `replay` clears the state and re-enables it.
- **A specific message is missing** → check `senders denied` / `malformed` in the
  per-poll summary line, then `replay`.
- **The store is the only state.** Deleting `DATA_DIR` loses cursors, job queue
  and dedup; mail stays on the server, so re-ingest is possible but documents
  will be re-uploaded.

## Development

```bash
make lint          # ruff
make test-unit     # unit tests only
make test          # full suite; needs Docker (compose stack + PAPRA_REQUIRE_STACK=1)
```

Integration tests run a real IMAP server, Papra and an LLM stub; they are
deselected unless `PAPRA_REQUIRE_STACK=1` is set.
