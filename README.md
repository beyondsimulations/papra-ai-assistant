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
| `papra-suite check` | config + migrations, the parsed allow-list, and what was refused in the last 24 h; changes nothing |
| `papra-suite serve` | pollers, OCR and title workers, sweep loop |
| `papra-suite replay --mailbox-id ID [--from-uid N] [--dry-run]` | rewind the poll cursor |
| `papra-suite events [--limit N] [--mailbox-id ID] [--kind K] [--since YYYY-MM-DD] [--json]` | why messages and attachments were **not** ingested |

`check` ends with one line — `events (last 24h): sender_denied=3, upload_failed=1`
— because `check` is the command an operator already runs when something is wrong,
and "the mailbox polls fine but nothing arrives" should not need a second one. An
empty or fresh store prints `none`; it never fails the check.

`replay` is the answer to "I set the allow-list wrong for a week." Refused mail
was retired but never deleted, so rewinding re-reads it, and Papra's content-hash
dedup means already-ingested documents are not uploaded twice. It also
re-enables a mailbox that repeated failures had disabled, and refuses to move the
cursor forward.

### Where did my attachment go?

`papra-suite events` is the durable answer. The per-poll log lines and the summary
counters say *how many*; this says *which one, from whom, and why*, and it survives
log rotation. Output is one event per line, tab-separated, newest first, printing
whatever columns the store actually has. `--json` gives the record fields instead,
with `null` for absent rather than the display's `-`.

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

- **It is not an archive.** Rows are kept for 90 days and pruned at `serve`
  startup and after every sweep pass, so an empty result can mean "outside the
  window". It names its own window when it prints nothing, and it restates the
  filter you asked with.
- **It is not a delivery log.** Success is never recorded, but a message can still
  appear here for the part it skipped: a mail whose invoice PDF uploaded and whose
  `.zip` did not leaves a row, and that row does not mean the mail failed. A
  persistent upload failure also writes one row per poll for the same uid, so count
  rows per uid before concluding there were five bad messages.
- **Absence here is not absence in Papra.** No rows is equally consistent with
  everything having ingested, with the 90 days having passed, and with the event
  write itself failing (which is logged, never stored).

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

## Recovery

There is no backup tooling, and nothing below pretends otherwise. "Back up
`state.db` periodically" is only a strategy if the copy is restorable and you know
what it returns, so this says exactly what a restore recovers and what no restore
can.

### `state.db` is lost or corrupt

The SQLite file under `DATA_DIR` is the only state this software keeps. Papra holds
the documents; everything about *how* they got there lives in that one file.

| Lost with `state.db` | Consequence |
|---|---|
| Organisations and mailboxes (host, user, **password**) | re-add every mailbox, with a password you have to still have |
| Poll cursors and `UIDVALIDITY` | every mailbox is read from the beginning again |
| Content dedup hashes and the seen-document table | everything still in an inbox is attempted again |
| Job queue (OCR and title work) | pending and in-flight work is gone |
| Settings | today that is only the sweep's resume offsets (`sweep_offset:<org>`), so the sweep re-scans from page one |
| `log_lines` ring buffer | the in-app log history |
| `ingest_events` | the 90-day "why was this skipped" window |

**The documents themselves are safe in Papra**, and re-ingest converges instead of
duplicating: Papra dedups by content hash per organisation, so a re-uploaded document
answers `409 already_exists`, which this pipeline treats as already-ingested success
rather than as an error or a second copy.

Two different recoveries, worth not confusing:

- **You have a known-good copy of `state.db`.** Restore it and everything in the
  table above comes back — credentials, cursors, dedup state, pending jobs. Do it
  with `serve` stopped.
- **You have no copy.** Re-add the orgs and mailboxes and let the pollers and the
  sweep run. This is the cheaper path than it sounds, because the documents never
  left Papra; what you rebuild is routing, not content.

Three consequences worth knowing before it happens:

- **Mailbox passwords live only in that file.** Losing it means having them again to
  put the mailboxes back — which is also the reason to treat the file, and any copy of
  it, as a secret.
- **Re-ingesting mail does not restore the processing.** A document already in Papra
  is answered with a 409, and a 409 deliberately does not enqueue OCR or titling.
  Documents that are already in Papra are the **sweep's** job, so missing content
  comes back through sweep passes, one bounded page at a time: a full re-OCR of the
  archive at LLM cost, spread over however many passes it takes.
- **A copy of the file is what a restore consists of**, plus the credentials you kept
  somewhere else. The database runs in WAL mode, so a bare `cp` of a live one can
  miss transactions still sitting in `-wal` and can catch a torn state: copy with
  `serve` stopped, or use SQLite's own online backup (`VACUUM INTO` or the backup
  API) which takes a consistent snapshot.

### A sender was wrongly refused

`papra-suite events --kind sender_denied` shows what was refused, when, and the
`From:` text that was refused. Fix `PAPRA_ALLOWED_SENDERS`, then replay. The mail is
still on the server, because a refusal marks it `\Seen` and never deletes it.

`replay --from-uid N` re-reads mail **strictly after N**, so to get the message back
whose row says `uid=18413`, pass `--from-uid 18412`. Passing `18413` is a valid
rewind that does not re-read it — the one mistake this interface makes it easy to
make. Omitting `--from-uid` re-reads the whole mailbox, which is the safe default
when several messages were refused.

The `uid` in a row is only meaningful together with the `uidvalidity` printed beside
it. Quote both when you go looking for the message: after a mailbox rebuild the
numbering restarts and the same uid can name a different message.

### OCR or title jobs are stuck

`serve` rewinds jobs left `running` when it starts, so a container killed
mid-document retries that document. The `jobs` table holds the rest — `status`,
`attempts` and `error` say what has been tried.

`papra-suite events` answers the question before that only partly: a row can tell
you a sender was refused, an attachment skipped, or an upload rejected, but the
absence of a row does **not** mean the document arrived — it is equally consistent
with success, with the 90-day window having passed, or with the event write having
failed. For "is this document in Papra", ask Papra.

### One caveat over all three

**A content write to Papra is not known to be durable.** Papra runs its own content
extraction on every upload and writes the same `content` column this pipeline writes
after OCR. The two race, and measurements on the pinned image show the later writer
winning: an OCR patch that lands while Papra's extraction is still in flight is
simply gone, and content search then finds nothing for that document. So nothing here
promises "your documents come back with their OCR text" — the documents come back,
the text may not. The mechanism and the numbers are in the characterisation spec's
addendum,
`docs/superpowers/specs/2026-11-16-ocr-content-search-intermittent-characterisation-design.md`.

## Development

```bash
make install-uv       # deps into .venv (uv); `make install` is the pip variant
make lint             # ruff over the tree (measurement probes are excluded)
make test-unit        # 447 tests, no Docker, ~18 s
make stack-up         # scratch Papra + GreenMail, images pinned by digest
make ci               # lint + unit + integration against the running stack
make test-ci          # CI shape: up -> run everything -> down -v   (DESTRUCTIVE)
make stack-digests    # pinned digests vs what upstream serves now
```

Three things about these targets that are worth knowing before you trust one:

- **`make ci` assumes the stack is already up**, and `make test-integration` never
  brings it up or takes it down. `make test`/`make test-ci` end in `down -v` because
  that is right in CI, and on a machine where this is also the scratch stack someone
  is measuring against, it is a way to lose work mid-investigation. Papra's data does
  not survive `stack-down` either — the compose file declares no named volumes — so
  every one of these stacks is disposable.
- **`PAPRA_REQUIRE_STACK=1` is what makes a green suite mean something.** Without it
  the integration tests *skip* when the stack is absent and the run still exits 0.
  With it, an unreachable stack fails. Verified against the shipped helpers: a dead
  port is not reported ready, the flag turns that into a failure, and without the
  flag it skips.
- **Tools come from `.venv/bin/` by default**, so `make lint` cannot silently check
  against some other project's ruff — and `make install`'s pip does not exist on a
  `uv`-bootstrapped venv, which is why `install-uv` is there. Use a system
  environment with `make lint VENV=`.

Integration tests run a real IMAP server and a real Papra (the LLM is an injected
stub, so no model container is needed); they are deselected from the default run by
the `integration` marker and required by `PAPRA_REQUIRE_STACK=1`.
