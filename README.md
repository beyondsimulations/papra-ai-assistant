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
| `PAPRA_INDEX_METADATA_DELIVERY` | default `false` — see "Handing the title, date and note over" |
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
| `papra-suite metadata [--restore-auto \| --force] [--org-id ID] [--document-id ID] [--field name\|date\|notes] [--dry-run]` | deliver Kreuzakt's title, date and note into Papra, or hand a protected field back to automatic delivery |

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
| `PAPRA_INDEX_METADATA_DELIVERY` | `false` | the bridge indexes documents and never writes `name`, `documentDate` or `notes`. On means it writes all three — but only where it can prove the field is still ours, see "Handing the title, date and note over". Papra's own enrichment keeps running regardless: no flag here stops it |

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
2. Let the corpus index. For what the sweep can see, the sweep is the backfill: every
   document the ledger has never held is queued as an `index` job, and `check`'s
   `refs`/`current` counts are how far it has got. Read the limit before trusting the
   number. The sweep skips documents this suite has already marked *seen*, and
   seen-with-no-ledger-row is a real population: every PDF emailed into the intake address
   while the legacy writers were in charge was marked seen by the legacy OCR enqueue and
   never ledgered, and oversized skips are marked the same way. Those documents appear in
   **neither** `refs` **nor** `current`, and `check` cannot currently reveal them — it has
   no count of Papra documents the ledger has never held. So it is possible to follow these
   steps, read `112 current`, and still have months of emailed invoices outside the index.
   The enrolment path is `papra-suite backfill`, which is Task 15 and not in this build;
   Task 12 gives `check` an `unledgered=N` count so this blind spot becomes a number
   instead of a paragraph. Until both exist, treat the indexed corpus as "everything
   uploaded or discovered since the ledger started watching", and write that into the
   handover notes rather than a bare `current` count.

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
index: 118 refs, 112 current, 96 delivered, 3 extraction-empty, protected_name=4 protected_date=61 protected_notes=2, 0 repairs(24h), 0 cleanup-pending (failed 0)
content writer: suite ocr worker (legacy)
metadata writer: suite title worker (legacy)
```

- `refs` counts every row the ledger has ever held, deletions and stale bindings included;
  `current` those bound to the live index generation; `delivered` those we last recorded
  sending text for. `delivered` is a receipt for what this bridge says it sent, *including
  the blank extractions it deliberately sent nothing for* — it is not a statement about what
  Papra's column holds now, because Papra's own extraction can overwrite us afterwards. The
  convergence pass, not this number, is what says Papra holds the text. `current` below
  `refs` is stale bindings plus documents queued for cleanup, which is what the last two
  fields split apart; `delivered` below `current` means documents waiting for the flip or
  for a delivery that has not run.
- `extraction-empty` counts documents the index holds *no* text for — a scanned page with
  nothing on it, or an extraction that failed. These are deliberately never written, so
  this is not a queue to drain. It is the count to look at when a document "should have
  been OCR'd and wasn't".
- `repairs(24h)` is how often the convergence pass wrote Papra's text back inside a rolling
  day — a count of repairs spent, not of divergences noticed, so a document Papra refuses
  to accept writes for does not inflate it and a document the pass has stopped fighting does
  not add to it either. Non-zero right after the handover is expected once and then should
  go quiet; sustained non-zero means the text we send is not the text that stays.
- `cleanup-pending (failed N)` is upstream deletes the ledger still owes, with the part the
  retries stopped on called out separately. A `failed` row can leave a deleted document in
  the index, so search still returns it; nothing in this build replays it by hand, so a
  non-zero `N` is a number to report rather than one that drains on its own.
- `protected_name/date/notes` count the fields this bridge has stopped writing because Papra
  holds a value it cannot attribute to itself — usually a rename or a note the user typed.
  Protection is permanent and carries no record of *why* it was set, so one legitimate rename
  and a corpus-wide lock read identically from anywhere else; these three numbers are the
  only place the difference is visible without reading `metadata --dry-run` line by line. A
  `protected_date` in the tens is normal (Papra fills `documentDate` on upload). A
  `protected_name` count that jumps after a flag flip is not: see the metadata handover.

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

`metadata writer:` is the second line, and it names who writes Papra's `name`,
`documentDate` and `notes`. Those three change hands on their own flag, in their own order,
and one of the four arrangements is deliberately **not** refused:

| Printed | Means |
|---|---|
| `suite title worker (legacy)` | this suite's title worker writes the title; the bridge does not (the default) |
| `index bridge` | the bridge writes all three fields and the legacy loops are not running |
| `none (handover window)` | neither of our two writers writes them, so a document keeps whatever Papra's own enrichment makes of it |
| `two writers (title queue draining)` | delivery is on while the legacy worker is still running: a state to pass through, not one to settle into |

The fourth is reported rather than refused, unlike two writers of `content`, for three
reasons. Two writers of a title cost a guess at a title, and the ledger converts the race
into a decision on the next pass — the worker's write differs from what we recorded, so the
field is protected and we stop touching it, which is recoverable with
`papra-suite metadata --restore-auto`; two writers of `content` can put a blank page over a
year of text. Refusing the combination would also forbid the only workable order, because
`legacy_workers` has to stay on while the queues drain and the flag that drains them is the
one being set. And the second writer is on a path this flag switches off **twice**: while
metadata delivery is on, `on_job_done` stops enqueuing the `title` job that an OCR completion
used to produce, for documents from every intake path, sweep and mailbox alike, and
`run_title_job` refuses the ones already sitting in the queue — it skips them and marks them
done, exactly as it skips a job whose content has moved on.

Both halves are needed, and the consumer half is what this section got wrong when it was first
written. Stopping only the producer leaves the `title` rows already `pending` or `running` at
the moment of the flip still claimed, and a title that worker writes is recorded nowhere in the
ledger: the next metadata pass finds a `name` that is neither the one we wrote nor the one
Papra had at first sight, calls it a human edit, and protects the field **permanently** —
byte-identical to a genuine rename, because protection is one bit with no provenance, and the
only bulk recovery is `metadata --force --yes-i-mean-it`, which overwrites every real rename in
scope. With both halves in place the state is harmless, and the line is printed anyway because
the two flags really do name two writers until `PAPRA_LEGACY_WORKERS` goes off. What can still
cross the flip are jobs that got past their gate check before the flag moved and are still
mid-flight — hashing Papra's content, or waiting on the model, with the PATCH not yet sent; the
not-ours verdict above is the containment for those writes.

### Handing the title, date and note over

The index holds a title, a date and a note for every indexed document, produced by the model
sitting behind the index. `PAPRA_INDEX_METADATA_DELIVERY=true` puts them into Papra's
human-visible `name`, `documentDate` and `notes`. It is a separate flag from content delivery
on purpose: the text is a machine's transcription nobody reads in the UI, while the title is
the thing the user renamed last Tuesday, and the two are handed over on different days.

**What "on" means per field is: write it only where we can prove the field is still ours.**
Papra offers no conditional write, so everything rests on what the ledger remembers, and the
rules are worth knowing before you flip because they decide what you will see in the logs:

- The baseline for `name` is **Papra's own name for the document at the moment we first saw
  it**, stored in the ledger as `papra_original_name`. It is *not* Kreuzakt's
  `original_filename`, which holds our own upload name for every document in the corpus — we
  named the file when we uploaded it, so measuring a human's rename against it measures
  nothing. For `date` and `notes` there is no baseline to have, so the first write requires
  the Papra value to be **empty**: Papra runs its own date detection, and a date we cannot
  attribute might be the user's or Papra's.
- A field we wrote and Papra no longer holds, or a value that was there before we ever wrote
  one, is **protected**: we stop writing it, permanently. Nothing in the delivery path ever
  lifts a protection, and the protection is logged once per field, when it is set.
- Dates are compared as days, because a `documentDate` we send as `2025-10-01` is stored and
  read back as `2025-10-01T00:00:00.000Z`. A model date that is not a bare ISO day is
  skipped rather than sent — Papra answers 400 for free text, and that 400 would take the
  title and the note down with it. What this suite checks is the **shape**, not the source: it
  does not know whether a given day was read off the scan or stamped when the index finished the
  document, and it has no opinion about which one deserves to be the user's date field. So
  before the flip, read `papra-suite metadata --dry-run` and count the `date=written` lines —
  that is how many documents are about to receive a date they never had.
- `notes` is truncated to this suite's own limit before it is compared, sent and recorded.
  What the ledger remembers is the string Papra received, not the model's longer original;
  recording the original would make our own truncation look like a human edit on the next
  pass.
- One PATCH per reference, carrying only the fields that changed, and never `content`: that
  column has its own gate and its own flag. A write Papra refuses records nothing, so every
  field in a refused request stays retriable.
**Our own upload name is never delivered as a title, on either side of the comparison.** A
Kreuzakt with no vision model configured derives the title from the filename, and the
filename is always ours: every document this bridge uploads arrives as
`papra-<documentId>-<hash8>.pdf`. On such a deployment the model's "title" *is* that string —
and measured on the pinned container, Papra's prettified version of it, so
`papra-doc-1-a1b2c3d4.pdf` comes back as `Papra Doc 1 A1b2c3d4`, with ` (1)` appended when the
name collided. Delivering it would rename the entire corpus to our own upload garbage and
then record that garbage in `written_name` as authoritative, so every later pass would defend
it. The pass refuses before it asks who owns the field: the verdict is `skipped` — not a human
edit, so it protects nothing, and not a write, so it records nothing — and a synthetic name
found in *Papra* is likewise never treated as ours to overwrite. The pinned container on the
test stack is exactly this keyless configuration, which is why the guard is covered against
the live service rather than only against a double.

**The two sides of that guard are not the same size, because only one of them is reversible.**
The recogniser matches a shape, not a registry, so it also refuses genuine names that end in an
eight-character hex-ish run: `Papra-Rechnung-20250101` looks like our upload suffix and will not
be delivered as a title.

On the candidate side that costs one skipped title and nothing else: the verdict is `skipped`,
no flag is set, and a narrower recogniser in a later build writes it.

On the Papra side the same shape answers `protected`, which is permanent: the bridge decides
somebody else owns that field and stops writing `name` on that document for good, until
`papra-suite metadata --restore-auto --field name` says otherwise.

The order, which is its own because the flags are separate and each reversible until the code
comes out:

1. Deploy with both delivery flags off and let the corpus index, as for content. Nothing about
   Papra's records changes.
2. Decide whether you want the bridge's titles at all, and look at what it would do first.
   Delivery is refused while the flag is off, so read the verdicts from the legacy side by
   checking the index directly, or turn the flag on in a test deployment and run
   `papra-suite metadata --dry-run`. The dry run prints one line per reference with
   `name=`/`date=`/`notes=` verdicts and writes nothing — not even a protection flag.
3. Set `PAPRA_INDEX_METADATA_DELIVERY=true` and restart, leaving `PAPRA_LEGACY_WORKERS` as it
   is. `check` prints `metadata writer: two writers (title queue draining)`, and that is the
   arrangement you want for a minute: the flag has to be on before the queue can stop being
   fed, because while it is off every OCR completion enqueues another title job.

   Flipping with rows still in the queue costs far less than it used to: a `title` job that
   *observes* the enabled flag skips, marks itself done and writes nothing, so those documents
   keep whatever title they had. The whole residual is that word "observes". A job that read the
   flag while it was still off and is mid-flight — hashing Papra's content, or waiting on the
   model, PATCH not yet sent — can land its write after the flip, and more than one job can be in
   that state. The window closes when the process actually stops, because a restart rewinds
   `running` rows to `pending` and the new process reads the flag first, so what gets through is
   what was already near its PATCH at the moment of the flip. The not-ours verdict is the
   containment for those writes, and `check`'s `protected_name` is how you see whether any landed.

   Draining **before** the flip lowers the chance of that race — in steady state the queue
   empties between OCR completions — but it is not a guarantee, because observing zero is not
   atomic with the next OCR completion enqueueing a title job and a worker claiming it before the
   restart. So the pre-flip drain is worth doing and proves nothing by itself. What no pre-flip
   count can do is *prove* zero: while the flag is off the queue refills behind you, so a count
   taken a minute early reads clean and means nothing. The proof is step 4, taken after the flip.

   **If content is already handed over** — you followed that section first, so
   `PAPRA_LEGACY_WORKERS` is `false` — one flag cannot satisfy both sections, and step 3's
   `two writers (title queue draining)` is not what you will see: `check` prints
   `metadata writer: index bridge`, because the loops that claimed `title` rows are gone. A
   restart does not bring them back and does not run the leftovers either; it only rewinds
   `running` rows to `pending`. Anything still queued is therefore inert, and step 4's count
   will never reach zero while it sits there. Delete it, the same way content's step 3 treats
   its own leftovers:

   ```bash
   sqlite3 "$DATA_DIR/state.db" \
     "DELETE FROM jobs WHERE kind='title' AND status IN ('pending','running');"
   ```

   Deleting is tidiness, not safety: with the flag on, a claimed title job would skip and mark
   itself done anyway. Leaving the rows costs nothing but a number that never drains.

4. Prove the second writer is out of work, counting only the rows that are still work:

   ```bash
   sqlite3 "$DATA_DIR/state.db" \
     "SELECT count(*) FROM jobs WHERE kind='title' AND status IN ('pending','running');"
   ```

   Two zeroes a minute apart. Counting every status proves nothing, because `completed` rows
   accumulate forever and that total never reaches zero. With the flag on,
   `on_job_done` refuses the ocr-done-to-title transition and `run_title_job` drops anything it
   still claims, so nothing this pipeline owns refills the queue. Two other readings are both
   ordinary: a count that keeps *growing* means something outside this pipeline is enqueueing
   `title` jobs, and a small count that never moves and never empties is the
   already-handed-over case above — rows no loop will ever claim again.

   `check` still prints `two writers` after this, and that is not a contradiction: the line is
   computed from the two flags, not from the queue, and while the legacy loops run they could
   still claim title work. It reads `index bridge` when `PAPRA_LEGACY_WORKERS` goes off, which
   is content's step and not this one.

5. Run the pass over what the index already holds, which the index worker only does for
   documents it indexes from then on:

   ```bash
   papra-suite metadata --dry-run    # read it
   papra-suite metadata              # write it
   ```

   Expect a lot of `protected` on `date`, and some on `name`. That is the rule working rather
   than a misconfiguration: Papra's own date detection fills `documentDate` on upload, and a
   date we cannot attribute is not ours to overwrite; a title Papra's enrichment rewrote after
   we first saw the document is likewise indistinguishable from the user's. `notes` is usually
   the one field that writes freely, because nothing but the user puts anything there.

### A human renamed it, and now the bridge will not

That is the intended behaviour, and it is permanent by design: the alternative is a nightly
job that quietly puts `papra-doc-1-a1b2c3d4` back over the user's filename. The way back is a
command rather than a SQLite incantation:

```bash
papra-suite metadata --restore-auto --dry-run     # what would change, per field
papra-suite metadata --restore-auto --field name  # do it, for one field
papra-suite metadata --restore-auto --document-id doc_1 --org-id org_1
```

`--restore-auto` calls the ledger's only un-protect and prints how many references it changed.
It reads no Papra and no Kreuzakt, so it works while either is down and while the delivery
flag is off — it is the command for the middle of the incident, and an escape hatch with a
network dependency in it is not an escape hatch.

Neither command alone returns a genuinely edited field to automatic delivery, and the order
matters:

```bash
papra-suite metadata --force --yes-i-mean-it --field name --document-id doc_1
papra-suite metadata --restore-auto --field name --document-id doc_1
papra-suite metadata --dry-run --field name --document-id doc_1    # name=unchanged
```

`--force` puts the index's value back over the human's and leaves the protection flag set;
`--restore-auto` then drops the flag; and the third command is what "automatic" means here —
`unchanged`, because Papra now holds what we sent, with the flag off. The opposite order
achieves nothing, and that is deliberate rather than a bug: clearing protection re-admits the
field to the ownership question, whose answer is still "Papra holds something we never wrote",
so the next pass protects it again. The flag was the symptom; the diverged value is the reason.
The CLI prints this order after a `--restore-auto` for exactly that reason.

`--force` is a different thing and the two are mutually exclusive on purpose. It delivers once
regardless of protection and **leaves the flags set**, which makes it safe to reach for when
the question is "is the index's value right after all?" rather than "stop protecting this
field forever". Because it overwrites text only the user can restore, it prints its plan and
exits `1` unless you add `--yes-i-mean-it`. `--org-id`, `--document-id` and `--field` narrow
either command; `--field` takes `name`, `date` or `notes` and nothing else — `content` is not
a metadata field and has its own flag, its own gate and its own handover section.

Three limits worth stating plainly, because all three look like bugs on the day they happen:

- **Papra's own enrichment keeps running.** No flag here stops it, and it can rewrite a title
  after our delivery. The next pass then reads a value that is not ours and protects the field:
  the enrichment has not lost to us, it has simply taken the field out of play. There is no
  convergence pass for titles — the verification pass converges `content` only — so the fix
  for "I want the index's title there" is `--force`, not a wait.
- **A document Papra says it has deleted is refused outright**, with a permanent failure rather
  than a retry. Changing a column resurrects a trashed document, so "helpfully" delivering
  metadata into one would un-delete something the user threw away. The delete propagation pass
  owns those references, not this one.
- **A permanent refusal fails the `index` job, and nothing re-asks on its own.** There is
  no separate metadata job kind: this pass runs inside the `index` job, so a permanent
  verdict here leaves an `index` row in `failed`. Since migration 004 that spent row no
  longer blocks the queue — the idempotency index covers `pending` and `running` rows, so
  a later request for the same bytes takes a row of its own and can be claimed — but the
  ledger still has to consider the document work. A reference left bound under the current
  generation is not work by that rule, so a restore, an unbind or a backfill that only
  enqueues what the ledger calls stale will walk past it. `check` cannot see any of this,
  because its counters are all ledger-derived and a job moving from `done` to `failed`
  changes none of them; the worker's ERROR line is what names it, and naming the document
  again is an operator's act (`jobs --reset` is Task 15's).
