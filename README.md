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
| `PAPRA_KREUZAKT_URL` | the Kreuzakt index, e.g. `http://kreuzakt:3000`; unset means no index, search falls back to Papra |
| `PAPRA_INDEX_CONTENT_DELIVERY` | default `false` — see "Handing the `content` column over" |
| `PAPRA_INDEX_METADATA_DELIVERY` | default `false`, and **inert**: nothing reads it yet, so turning it on changes nothing at all |
| `PAPRA_LEGACY_WORKERS` | default `true` — this suite's OCR and title workers keep writing; flipping it off is the handover |
| `PAPRA_INDEX_MAX_ATTEMPTS`, `PAPRA_INDEX_POLL_INTERVAL_S`, `PAPRA_INDEX_WAIT_TIMEOUT_S`, `PAPRA_INDEX_QUERY_MAX_CHARS`, `PAPRA_INDEX_SEARCH_SCAN_ROWS` | optional tuning of the index pipeline |

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

**What changed once the handover is done** (see "Handing the `content` column over") is
not the race but what happens after losing it. With the bridge as the only writer this suite
controls, a `content` column that says something other than what the index holds is found by
the convergence pass — every six hours, a constant rather than a knob, and once at every
start because a stopped container is how text goes missing — and re-written, and a
document that flip-flops twice in a day is left alone at ERROR instead of fought. So the
promise becomes bounded rather than absent: text we delivered is text we re-apply within the
interval, unless we deliberately stopped repairing that one document.

Papra's own extraction remains a third hand on the column that nothing here stops, and the
pass treats what it writes as a divergence and overwrites it. That is the design rather than
a bug: once the handover is done the index is the authority for the column, and a Papra
extraction that disagrees with it is the thing being corrected. Until the handover, the
caveat above is the whole story.

## Development

```bash
make install-uv       # deps into .venv (uv); `make install` is the pip variant
make lint             # ruff over the tree (measurement probes are excluded)
make test-unit        # 447 tests, no Docker, ~18 s
make stack-up         # scratch Papra + GreenMail + Kreuzakt, images pinned by digest
make ci               # lint + unit + integration against the running stack
make test-ci          # CI shape: up -> run everything -> down -v   (DESTRUCTIVE)
make stack-digests    # pinned digests vs what upstream serves now
```

Three things about these targets that are worth knowing before you trust one:

- **`make ci` assumes the stack is already up**, and `make test-integration` never
  brings it up or takes it down. `make test`/`make test-ci` end in `down -v` because
  that is right in CI, and on a machine where this is also the scratch stack someone
  is measuring against, it is a way to lose work mid-investigation. Papra's data does
  not survive `stack-down` either — no volume is declared for it, so its database is
  the container filesystem — so every one of these stacks is disposable. Kreuzakt is
  the one exception: `kreuzakt-data` is a named volume, so it survives `stack-down`
  and `down -v` is what erases it.
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

### The test stack, and where the tests actually run

The stack is three containers now: Papra, GreenMail, and Kreuzakt — the extractor and
the only search authority
(`docs/superpowers/specs/2026-11-17-papra-record-kreuzakt-index-design.md`). Kreuzakt
ships no authentication, so it lives on a network of its own (`indexnet`) and, in
production, publishes no port at all. The reachability list is therefore short, and it
is written down rather than implied: **Kreuzakt is reachable from the suite container,
and nothing else** — that is the intended production reachability, and it is a statement
about configuration: no published port, and a network with exactly one other member.
What it does not mean is "isolated at the IP layer". Measured on this compose file
(the output is pasted in the comment on the `kreuzakt` service): Papra's service name
does not resolve from the Kreuzakt container, but Kreuzakt could still reach Papra's
*container IP* on the other bridge and got `HTTP/1.1 200 OK`. So nothing here should be
treated as a boundary that stops a container which already holds an address; the real
protection is that no outside process is ever handed one.

Integration tests run on atlas as a process on that host — `ssh atlas`, then
`PAPRA_REQUIRE_STACK=1 make test-integration` from the checkout there — because
`DOCKER_HOST=ssh://atlas` does not move the test process's network namespace. With it
set, `docker compose ps` reports the atlas stack while the pytest process still dials
`127.0.0.1` on the laptop it is running on, and every connection test then fails (or,
worse, passes against some other local container). A test process reaches containers by
being on the host that runs them, so that is how the suite is run:

```bash
# The whole command has to execute on atlas. `ssh atlas && make test-integration`
# is not it: that opens and closes an ssh session, then runs make on the laptop.
ssh atlas 'cd ~/papra-ai-assistant && PAPRA_REQUIRE_STACK=1 make test-integration'
```

### Deploying Kreuzakt alongside the suite

Kreuzakt joins the operator's Dokploy stack for `papra-suite`, not Papra's stack, so
restarting the archive does not restart the index. This repo ships only
`docker-compose.test.yml`, so the production shape is this snippet — the same service
block the test compose uses, with the `ports:` key removed and nothing else changed:

```yaml
  # The suite container joins the same network, so it reaches the index by service
  # name, and nothing outside this network has a name or a port to aim at.
  papra-suite:
    # ...
    networks: [indexnet]

  kreuzakt:
    image: ghcr.io/anaisbetts/kreuzakt@sha256:4a0109d03d663338fd967662aa0151ec7684d2ba31cdfd12f1bf4f3901b1cd8d
    platform: linux/amd64          # upstream publishes amd64 only; atlas is x86_64
    environment:
      DATA_DIR: /data
      OCR_VLM_BASE_URL: "${OMLX_BASE_URL:-}"
      OCR_VLM_API_KEY: "${OMLX_API_KEY:-}"
      OCR_VLM_MODEL: "${OMLX_OCR_MODEL:-}"
      OPENAI_BASE_URL: "${OMLX_BASE_URL:-}"
      OPENAI_API_KEY: "${OMLX_API_KEY:-}"
      METADATA_LLM_MODEL: "${OMLX_TEXT_MODEL:-}"
    volumes:
      - kreuzakt-data:/data
    networks: [indexnet]
    healthcheck:
      test: ["CMD-SHELL", "bash -c 'exec 3<>/dev/tcp/127.0.0.1/3000'"]
      interval: 3s
      timeout: 2s
      retries: 40

volumes:
  kreuzakt-data:

networks:
  indexnet:
```

And in that same stack, the suite is told where its index lives:

```bash
PAPRA_KREUZAKT_URL=http://kreuzakt:3000
```

Three things this shape depends on, all of them cheap to get wrong:

- **`indexnet` is what makes `http://kreuzakt:3000` resolve**, and it resolves for the
  suite only. The test compose adds one thing the snippet above does not have —
  `127.0.0.1:12400` — solely because the test client is a host process; production has
  no listener, so its behaviour is the tested behaviour except for that socket.
- **The `OMLX_*` variables must be set** to the Mac Studio's tailnet address (an IP or
  an explicit host entry: container DNS will not resolve a MagicDNS name). Left empty,
  Kreuzakt still starts and still answers, keyless — it just extracts text layers and
  titles from filenames, which looks like a working index and is not one.
- **`kreuzakt-data` is disposable, by decision.** It holds Kreuzakt's copy of every
  original and its own queue; the documents and the index ledger live in Papra and in
  `state.db`. Losing the volume costs VLM minutes to rebuild, nothing else.

### Handing the `content` column over

Until this section applies, it is this suite's own OCR worker that writes a document's
text and the bridge only feeds the index. Two switches move that column to the bridge, and
both are environment variables on the suite container: `serve` writes them into the
`settings` table on every start, so the environment is what you change and the next restart
is what applies it.

| Variable | Default | Off means |
|---|---|---|
| `PAPRA_INDEX_CONTENT_DELIVERY` | `false` | the bridge indexes documents and never writes `content` |
| `PAPRA_LEGACY_WORKERS` | `true` | this suite's OCR worker and title writer stay in charge, and new documents still enter the `ocr` queue. It does not touch Papra's own extraction, which no flag here stops |
| `PAPRA_INDEX_METADATA_DELIVERY` | `false` | nothing today: the flag is seeded and read by no code, so setting it true changes neither tags nor titles. Papra's enrichment keeps writing them and the legacy title writer is stopped by `PAPRA_LEGACY_WORKERS`, not by this flag — do not plan a handover step around it |

**Drain the legacy queue before you flip, and not for tidiness.** Three things write
Papra's `content`: this suite's OCR worker, this bridge, and Papra's own extraction, which
runs on every upload and on `organizations:update` and which nothing here can stop. The two
switches move the column between the first two; the third is why the convergence pass exists
at all. With the suite worker and the bridge both enabled, a delivery can land while a
document's own OCR is in flight and be erased a moment later, with no error anywhere —
measured on the pinned image, the later write simply wins. That is why the combination is
refused at startup rather than treated as a configuration choice, and why the handover is an
ordered operation:

1. Deploy the build with both flags off. Nothing about Papra's records changes; documents
   are indexed and the index fills.
2. Let the corpus index. The sweep is the backfill: every document the ledger has never
   held is queued as an `index` job. `check`'s `refs`/`current` counts are how far it has
   got, and the queue's own state is in `state.db`:

   ```bash
   sqlite3 "$DATA_DIR/state.db" "SELECT kind, status, count(*) FROM jobs GROUP BY 1, 2;"
   ```

   Wait for `ocr` rows in `pending` or `running` to reach zero **while the legacy loops are
   still running**, so the worker drains its own backlog. The convergence pass ignores
   pending work, and a delivery landing while an OCR job is in flight is the race above.
3. Set `PAPRA_LEGACY_WORKERS=false` and restart, then prove the same query reads zero twice a
   minute apart. You are now in the handover window: `check` prints
   `content writer: none (handover window)` and `serve` logs the same at ERROR. Neither of our
   two writers touches `content` in this state, and a document uploaded now keeps only what
   Papra's own extraction makes of it — which is why this is a step you pass through and not a
   configuration you settle into.

   Anything still in `ocr` at this moment is never claimed again: the loops that claimed it
   are gone, and a restart only rewinds `running` rows back to `pending`. Those documents
   still get their text, from the bridge, once step 4 runs — so leftover rows are cruft to
   delete rather than work to wait for. Step 2 exists for the documents a worker is writing
   *right now*, not for the ones it has not reached.
4. Set `PAPRA_INDEX_CONTENT_DELIVERY=true`, restart, and read `check` before `serve`, since
   it prints exactly what `serve` would do:

   ```bash
   papra-suite check     # content writer: index bridge
   papra-suite serve
   ```

   Setting both variables in one recreate is not a shortcut you are being talked out of:
   while `legacy_workers` is still true, the two-writer refusal stops the process from
   starting at all, so this order is not tidiness but the only sequence that boots. What
   step 3's proof is for is the interval the refusal cannot see — a queue that is half
   drained while the loops have already stopped claiming, which nothing refuses for you.

**`check` reports the handover in the same breath as the config**, because the two
questions an operator asks — "how far did this get" and "should I have restarted" — are
one question. The lines are appended to what `check` already prints:

```
index: 118 refs, 112 current, 96 delivered, 3 extraction-empty, 0 repairs(24h), 0 cleanup-pending (failed 0)
content writer: suite ocr worker (legacy)
```

- `refs` counts every row the ledger has ever held, deletions and stale bindings included;
  `current` those bound to the live index generation; `delivered` those whose text Papra
  holds as we last sent it. `current` below `refs` is stale bindings plus documents queued
  for cleanup, which is what the last two fields split apart; `delivered` below `current`
  means documents waiting for the flip or for a delivery that has not run.
- `extraction-empty` counts documents the index holds *no* text for — a scanned page with
  nothing on it, or an extraction that failed. These are deliberately never written, so
  this is not a queue to drain. It is the count to look at when a document "should have
  been OCR'd and wasn't".
- `repairs(24h)` is how often the convergence pass found Papra holding something else.
  Non-zero right after the handover is expected once and then should go quiet; sustained
  non-zero means a second writer is alive somewhere.
- `cleanup-pending (failed N)` is upstream deletes the ledger still owes, with the part the
  retries stopped on called out separately. A `failed` row can leave a deleted document in
  the index, so search still returns it; nothing in this build replays it by hand, so a
  non-zero `N` is a number to report rather than one that drains on its own.

`content writer:` then names the arrangement you are actually running, in one of three
strings:

| Printed | Means |
|---|---|
| `suite ocr worker (legacy)` | this suite's OCR worker writes content; the bridge does not (the default) |
| `index bridge` | the bridge writes content and the legacy loops are not running |
| `none (handover window)` | delivery off and legacy off: neither of our two writers writes content, so a document uploaded now keeps only whatever Papra's own extraction produces and never gets the index's text |

The fourth arrangement, `two writers`, is never printed by a working program: `check`
exits `2` with a `configuration error` naming the two variables to set, and `serve`
refuses before it opens a connection to either service or starts a task — the refusal runs
before the clients are constructed, so nothing is sent to Papra or the index on the way
out. A `none (handover window)` is logged as an error by `serve` and is still allowed to
run, because search and the ledger keep working and a restart with the flags corrected is
all it takes; that window is a state you pass through, not one you configure on purpose.
