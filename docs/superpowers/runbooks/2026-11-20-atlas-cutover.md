# Atlas cutover runbook — Kreuzakt index bridge

**Who runs this.** Tobias at a keyboard on `atlas`. The controller steers each step: it says which
sub-step to run, reads the output with you, and decides whether to go on. Nobody runs two
sub-steps ahead of that decision.

**What is being cut over.** Papra stops being the thing that reads your documents. Kreuzakt — an
unpublished container on a private Docker network — becomes the extractor and the only search
authority; the suite becomes the delivery worker that writes Papra's `content`, `name`,
`documentDate` and `note` from what Kreuzakt extracted; and an MCP listener on loopback exposes
that index to `pi` and `hermes`. Six steps, each of which can be stopped and, through Step 5, each
of which can be reversed.

**Build.** `papra-suite` at the HEAD this runbook was written against (branch
`kreuzakt-index-bridge`). Every `papra-suite …` command below was read out of that build's own
`--help` output; the capture is quoted in
`.superpowers/sdd/2026-11-17-kreuzakt-index-bridge/task-16-runbook-report.md`. If a command is not
in `--help`, it is not in this file — and if your screen and this file disagree, the screen is
right: stop and report the difference rather than improvising a flag.

---

## How to read this

Every sub-step has four parts, and they are always the same four:

- **Why** — what this accomplishes, and what it is protecting against.
- **Do** — the command, or the screen to touch.
- **Expect** — what the output says when the step worked, quoted or described exactly enough to
  compare against.
- **If not** — what to do instead. This is not a troubleshooting appendix; it is part of the step.

Three conventions:

- **`ATLAS — verify on the box`** marks anything this file cannot know: Dokploy's screen paths,
  where the volumes actually live, what Papra's UI shows, and what `tailscale serve` forwards. The
  machine's owner reads the screen and types what it says into this runbook's margins. Nobody
  pretends a laptop knew.
- **No raw `sqlite3`.** State changes go through `papra-suite`; the only SQL this file allows is a
  read-only `SELECT`, and the README carries the one documented exception (a wrongly-refused
  sender, and the `ALTER TABLE … ADD COLUMN` a stale checkout needs).
- **Exit codes are output.** `0` means the thing is true, `1` means it is not yet true or was
  interrupted, `2` means it was refused. Do not chain these commands with `&&` in a way that turns
  a refusal into a silent skip, and when you want the text of a refusal, let it print: the
  "command not found" case must be visible as itself.

## What a laptop pilot does not prove

A pilot on the laptop is worth doing, and it proves less than it looks like it proves. The OCR path
needs a working vision model; with `OMLX_API_KEY` unset the extraction service cannot run at all,
so **a local pilot can only exercise the text-layer path** — digital PDFs that already carry text.
Your archive is **scans**. A pilot that "passes" on digital PDFs has tested the path your corpus
does not use, and says nothing about the only thing this cutover exists to fix.

What a laptop pilot *is* good for: the shape of the commands, what a drained queue looks like, what
the two-writer refusal says, what a bound reference and a delivered document look like in
`papra-suite ledger --status`, and what a scoped token can and cannot see (Step 6.4). It is a
rehearsal of the keyboard, not a rehearsal of the archive.

---

## Step 0 — preflight: what has to be true before the first document moves

Nothing in Step 0 cuts anything over. It exists because three of the five things it checks fail
silently — an empty interpolation, a default endpoint, a missing volume — and all three are much
more expensive to find after the corpus has been indexed.

### 0.1 The images are pinned, and the suite is the new code

**Why.** Kreuzakt speaks an HTTP API with no version negotiation. An image that moved under the
cutover turns every later symptom into "did they change it, or did we?".

**Do**

- Deploy the suite at this branch's HEAD into the `papra-suite` Dokploy service (ATLAS — verify on
  the box: which service, which build path, whether the install step uses
  `requirements-prod.lock.txt` — `mcp==2.3.0` is a new direct dependency, and an install that
  reads only `requirements.txt` will produce a `serve` that cannot answer MCP at all).
- Add the `kreuzakt` service from the README's production snippet to the **`papra-suite` stack,
  not Papra's**, with the `ports:` key absent, `platform: linux/amd64`, `kreuzakt-data:/data`, and
  the suite on the same `indexnet`.
- Set `PAPRA_KREUZAKT_URL=http://kreuzakt:3000` in the `papra-suite` environment.
- With both delivery flags at `false` (or absent) and `PAPRA_LEGACY_WORKERS` at `true` (or absent),
  start both services.

**Expect** `papra-suite check` to finish with exit `0` and print the handover lines naming the
**legacy** writers:

```
  content writer: suite ocr worker (legacy)
  metadata writer: suite title worker (legacy)
```

That is the correct starting state: the new code runs, the old code still writes, nothing has been
handed over.

**Note what is *not* in `check`'s output.** This build's `check` prints no `kreuzakt:`
line, and by design it opens no Papra or Kreuzakt client — so nothing here proves the
suite can reach the index. Step 0.3 proves the extractor is pointed where you want it;
Step 1.3 is the first moment the *suite's* path to it is proven, by a document that
actually gets delivered. Keep that order in mind when a later step says "if Kreuzakt is
down": the evidence is the queue and `ledger --status`, not a line in `check`.

**If not**

- `papra-suite: command not found` — first find out which name this build answers to, because the
  two forms are not the same claim. `papra-suite …` is the name the README uses; the module form
  `python -m papra_suite …` is the one this file was validated with, and it works from a checkout
  with nothing installed. If only the module form works, use it for every command here — the
  subcommands and flags are identical. If *neither* is the new code, get `--help` out of the running
  container and compare its `usage:` line with Appendix A.4: with `mcp` or `mcp-token` missing from
  that list you are running an older image, and every later step in this file would be guessing.
- A Kreuzakt that is not answering — this build's `check` has no `kreuzakt:` line to say so, so the
  real symptoms are later: a `backfill` whose `stale` count never falls, and
  `deletions: never ran (the sweep does this once per pass, with kreuzakt up)` still in `check` after
  the sweep has had a pass. Both mean the index is not answering; go to Step 0.3. If you *are*
  reading a `kreuzakt: down` line, you are running a different build from the one this file was
  written against: stop and say so.
- `degraded` in the health body from Step 0.3 with a nonzero backlog — the extractor is not
  answering; see Step 0.3's own **If not**.

### 0.2 The volume is a volume

**Why.** `state.db` holds the ledger that maps your archive to the index, the token hashes, the
seen table, and the queue. Losing it is survivable (README, *`state.db` is lost or corrupt*) and it
costs VLM hours and every token you minted. It should be lost on purpose, never by accident.

**Do** (ATLAS — verify on the box, and write what you find into this line: the suite's
`DATA_DIR` is mounted from ____________________ , and Papra's database is mounted from
____________________ )

- Confirm the suite's `DATA_DIR` path is a mount and not the container filesystem.
- Confirm Papra's database path is a mount too, and that your backup job covers it.
- Confirm `kreuzakt-data` is a named volume — and understand that it is the one volume you are
  allowed to lose: it holds Kreuzakt's copy of each original and its own queue, and rebuilding it
  costs extraction time and nothing else (README, *Deploying Kreuzakt alongside the suite*).

**Expect** both real paths, and a backup of Papra's database that you have restored at least once.

**If not** — if either is the container filesystem, **stop the cutover and fix the mount first**.
This is the one preflight item that is not negotiable, and the reason is concrete: the test stack
in this repo ships Papra with no volume at all, so `docker compose down` deletes that archive's
database and the Makefile says so in a comment. If you cut over onto a configuration where that is
true on the server, the first routine redeploy after Step 1 destroys the archive that the index was
built to serve. Fix the mount, take the backup, then come back to Step 0.3.

### 0.3 Prove where Kreuzakt sends your documents

**Why.** This is a privacy decision, not a configuration detail, and it belongs here — before the
first document is uploaded — because after the corpus is indexed the only fix is a rebuild and a
conversation about what left the tailnet.

Kreuzakt's HTTP API reports the endpoints it will use for inference, and **its baked-in default is
`https://openrouter.ai/api/v1`**. The production snippet in the README sets its base URLs as
`OCR_VLM_BASE_URL: "${OMLX_BASE_URL:-}"` and `OPENAI_BASE_URL: "${OMLX_BASE_URL:-}"`, and `${VAR:-}`
means an unset variable becomes an **empty string, not an error**. So an environment that does not
carry `OMLX_BASE_URL`, `OMLX_API_KEY`, `OMLX_OCR_MODEL` and `OMLX_TEXT_MODEL` does not fail: it
starts, it answers, and it points at a public inference router. For this archive that means scanned
bank statements, invoices and insurance documents leaving the tailnet to a third party you did not
choose.

**Do** — on atlas, inside the stack network (ATLAS — verify on the box: how you get a shell on that
network; the Kreuzakt image itself has no `curl`, so run it from a container that does, or from a
host path that reaches it):

```bash
curl -s http://kreuzakt:3000/api/health | python3 -m json.tool
```

**Expect** a JSON object whose `ocr_base_url` and `openai_base_url` both name **the operator's own
host** — the Mac Studio's tailnet address, or whatever endpoint you decided on deliberately.

**Forbidden** — any value containing `openrouter.ai`. If either field names it, stop: the
environment is empty and the default is in force. Set the four `OMLX_*` values in the stack
(ATLAS — verify on the box: where Dokploy keeps them, and whether they reach this service),
recreate the container, and read the health endpoint again.

**If not** — two honest outcomes beyond the fix:

- The endpoint you do want is not reachable from the container. Container DNS will not resolve a
  MagicDNS name, so use an IP or an explicit host entry (README, same section). A Kreuzakt that
  cannot reach the extractor is better than one that quietly reaches somebody else: it fails loudly
  with an empty queue backlog and `extraction-empty` counts.
- You decide, deliberately, that a hosted extractor is acceptable for this archive. That is your
  call to make, not this file's. If you make it, write down here, in this runbook, which documents
  you are willing to send and what the backup's retention is — and then re-read Step 4 before
  Step 5, because the fallback you are deleting is the one that would have kept this inside.

**The literal, because the failure it produces is misleading.** The index's inference base URL has
to carry the `/v1` path segment: with the same key, `OCR_VLM_BASE_URL=https://<host>` returns **401**
and `OCR_VLM_BASE_URL=https://<host>/v1` returns **200**, measured against the operator's own host on
2026-11-20. A missing `/v1` therefore looks exactly like a revoked key, and the obvious response —
mint a new key — fixes nothing. Note that the two halves of this system want **opposite** shapes, so
do not copy one value into the other: the index is configured with a path-bearing URL, while this
suite's own `LLM_BASE_URL` is validated by `_require_base_url` and **refuses a path**, failing at
startup with `LLM_BASE_URL must not include a path, got: 'https://<host>'`.

### 0.4 The languages the extractor was told to read

**Why.** The corpus is German. Papra's OCR default is English, and the index's language setting is
the same kind of setting: whatever it is not told, it does not read.

**Do** — in the **Papra** service environment:

```
DOCUMENTS_OCR_LANGUAGES=deu,eng
```

and, if the stack has one, the same value wherever Kreuzakt's OCR languages are configured
(ATLAS — verify on the box: whether this build of Kreuzakt reads a language setting at all, and
where; the test compose sets the value on Papra only, so that is the shape this file can vouch for).

**Expect** the value present on both services after a recreate. It **replaces** the default rather
than adding to it, so `deu` alone would drop English — keep both unless you have decided otherwise.
The first use of a language fetches its traineddata, which is why the host needs outbound internet
once per language.

**If not** — a missing value is invisible in every later symptom: extraction still succeeds, on the
languages it knows. If German text comes back as garbled fragments or empty later, this line is the
first thing to re-read, not the last.

### 0.5 The log level is `info`, and there is no knob for it

**Why.** Since the oracle collapse, *"that id exists in a bucket this token is not granted"* is not
a fact the wire carries any more — `get_document`, `read_document` and `reindex_document` answer an
out-of-scope id with `not-found`, the same shape as a document that does not exist. The distinction
survives in exactly one place: an `INFO` line in the log that names the bucket and the token label.
If your log pipeline drops `INFO`, then after the cutover you cannot tell a wrong id from a
permissions problem, and Step 6.4 has no diagnosis to read.

**Do** — confirm that the platform's log view for the `papra-suite` service shows `INFO` records,
and that they are retained long enough to be evidence (ATLAS — verify on the box: Dokploy's log
screen, its level filter, its retention).

**Expect** `INFO` lines from the service — e.g. the worker lifecycle lines — visible next to the
warnings.

**If not** — turn the collector's level down to `info`, do not turn our logger up. There is nothing
here to turn: `setup_logging()` sets the root logger to `INFO` in code and this build ships **no**
`LOG_LEVEL` environment variable, so the only filter that can be dropping the line is the one
between the container's stdout and the screen you read. If a level filter is unavoidable for
volume, keep a window where `INFO` is kept for the duration of Steps 1–6, and say so before you
open the MCP listener.

### 0.6 The size ceiling, decided before anything hits it

**Why.** `PAPRA_MAX_DOCUMENT_BYTES` is one knob with one consequence, and it is the consequence
this build cannot undo for you: **at or over the ceiling a document is refused at the mailbox,
never enrolled in Papra, and therefore never indexed and never searchable** (README, *Documents
that are too big*). A mail attachment that is 2 MB over the limit is not slow or degraded; it is
not in the archive, and the sender does not get told.

**Do** — decide the number, then set `PAPRA_MAX_DOCUMENT_BYTES` in the `papra-suite` service.
`check` prints the ceiling in force either way, and the strict parse means an empty or misspelled
value is refused rather than quietly becoming unlimited:

```bash
papra-suite check
```

**Expect** the ceiling line, printed whether or not anything has been refused:

```
  oversized: none seen (limit in force 104857600 bytes) — over it a document is neither enrolled
  nor indexed: compress it and re-upload to papra
```

**If not**

- The limit named is one you did not choose — an interpolation lost the variable, exactly as in
  Step 0.3. Fix it and restart; the default here is 100 MiB (`104857600`), and a value the parser
  does not like (`10MB`, `1e8`, `-5`, `0`, empty) is a startup refusal naming the bad value, never a
  silent default.
- `oversized:` counts refusals since the process started. It says nothing about documents refused
  before this process existed, and it does not name them. To find what the ceiling has already
  touched, run `papra-suite backfill --dry-run` (it reports the oversized population), and treat
  the mailbox log as the record of the ones that never became documents at all.
- To raise the ceiling later: change it, restart, and re-run `papra-suite backfill` — that is how
  you reach what an earlier ceiling kept out. Do not shrink it; the check is against Papra's stored
  size, so a file that was legal last month can become illegal by the same rule, and it will keep
  its index row while its new mail does not.

**Measured on the operator's archive (2026-11-20).** `backfill --dry-run` was run against production
with a key whose permissions are `documents:read`, `tags:read`, `custom-properties:read`,
`organizations:read` — capability-enforced read-only, so nothing here could write: **1,183 PDFs**
across the four buckets (Herde 327, of which 9 are not PDFs; Katharina Illustration 285;
BeyondSimulations 79; Uni Hamburg 492), and **zero of them over the ceiling**. So the 100 MiB default
is not a live constraint on this archive: keep it, do not raise it, and revisit only if a scanner
starts emitting hundred-megabyte scans. The same dry-run listed the largest bucket's 492 documents in
half a second, which is what makes Step 1.3's twenty-document rehearsal cheap.

### 0.7 The queue baseline, before anything changes

**Why.** Every later step reads the queue. A baseline is what makes "this is normal" a comparison
instead of a feeling.

**Do**

```bash
papra-suite jobs --limit 20
papra-suite ledger --status
papra-suite check
papra-suite mcp-token --list
```

**Expect** (ATLAS — verify on the box, and paste these four outputs somewhere you will still have
them in Step 5): the legacy `ocr`/`title` queue at or near empty on a settled archive;
`mcp: disabled (nothing listens; set mcp_server_enabled=1 and give PAPRA_MCP_URL to start the
listener), tokens: 0 active, 0 revoked`; and `ledger --status` counts. If the corpus has never been
indexed, `unledgered` is the number of documents that will need a backfill — write it down, because
Step 3.1 measures the backfill against it.

**If not** — if the legacy queue is not settling now, fix that before the cutover. A cutover is the
worst possible moment to discover the queue was already stuck, because after Step 1 the writer of
`content` changes and "it was already broken" becomes impossible to establish.

### 0.8 The backup that makes Step 5 possible

**Why.** Steps 1–4 write; Step 5 deletes the code that could have written the old way. The thing
that lets you delete a fallback is having a copy of what the fallback produced.

**Do** — back up, and confirm you can read:

- Papra's database (the archive: documents, names, dates, notes, and the bytes)
- `state.db` — copy the file with the suite stopped, or use `VACUUM INTO` against the live
  database (README, *Recovery*)
- the `OMLX_*` values you set in Step 0.3, in a place that is not the Dokploy screen

**Expect** three artifacts you could restore today.

**If not** — do not open Step 5 without them. Steps 1–4 are all reversible with the tree unchanged,
and Step 5 is the one step where the reversible set gets smaller.

---

## Step 0b — measure what `tailscale serve` actually forwards

**Why.** The MCP listener refuses requests by `Host` and `Origin` before it looks at your token:
the Host check runs first, so a request that names the wrong host gets `421` and never reaches
authentication. That is the whole defence against DNS-rebinding, and it is also the way to lock
yourself out of your own listener — because what the listener sees is not necessarily what you
typed into `tailscale serve`. Whether the proxy forwards the public name, rewrites it to
`127.0.0.1:8765`, adds or strips `X-Forwarded-*`, and what `Origin` it sends with a POST, is a
property of your proxy configuration. Nobody working from a laptop knows it, and guessing it wrong
produces a listener that answers nothing and looks dead.

So: measure it, in the topology it will really run in, before there is a listener to be confused
about. Do this **now**, in Step 0, because by the time the listener is up you will have a broken
symptom to explain and this measurement will look like optional homework.

**Do** — run the shipped probe on atlas, on the same user and the same tailnet the listener will
use (it is stdlib-only; nothing to install):

```bash
python3 docs/superpowers/probes/2026-11-20-tailscale-headers/echo_headers.py
```

It listens on `127.0.0.1:8765` and prints, for every request, the Host, the Origin, the
`X-Forwarded-*` set, everything else, and the peer address it saw. **That port number has nothing
to do with the MCP listener's default port, which is also `8765`** — the coincidence is only
confusing if you run both at once, so don't. The script prints the commands to run and a table to
fill in; stop it with Ctrl-C once the table has rows.

Then, with `tailscale serve` pointed at `127.0.0.1:8765` (ATLAS — verify on the box: the exact
`tailscale serve` command or JSON, and where you saved it), send it traffic you control, at least:

- an unauthenticated `GET` from the tailnet
- an authenticated MCP-shaped `POST` (`Content-Type: application/json`,
  `Accept: application/json, text/event-stream`) — a token that does not exist is fine; you want
  the headers, not the answer
- one request whose `Origin` you set by hand, so you can see whether the header survives

**Expect** this table filled in with observed values, and nothing inferred:

| request | `Host` seen | `Origin` seen | `X-Forwarded-For` | `X-Forwarded-Proto` | `X-Forwarded-Host` | peer address |
|---|---|---|---|---|---|---|
| unauthenticated GET from tailnet | | | | | | |
| MCP-shaped POST from tailnet | | | | | | |
| POST with hand-set Origin | | | | | | |

Three rows you should be able to answer in words afterwards:

1. Is the Host the **public tailnet name**, or does the proxy hand over something else?
2. What is the Origin of an MCP client's POST — and note that Origin is **required** on POSTs, so
   an `Origin`-less request is refused with `400` before it is authenticated. A proxy that strips
   it produces a listener that no browser-shaped client can use.
3. Are there `X-Forwarded-*` headers, and do they agree with the socket? The listener trusts proxy
   headers only from loopback, so forwarded headers arriving from a non-loopback peer are a thing
   to know about before you have a rate limiter that reads them.

**If not** — there is no "if not": this step is a measurement, and it cannot fail. It can only be
skipped, and Step 6.3 is where that gets paid. If the probe never received a request, the proxy is
not pointed at `127.0.0.1:8765` and you have learned that cheaply.

**Record** the table here, in the runbook, before Step 1. Step 6.3 reads it and decides nothing
without it.

---

## Step 1 — content delivery on for one bucket

**Why.** This is the step that writes into the archive. Until now the bridge only read Papra and
wrote Kreuzakt; from here it also writes Papra's `content` column, which used to belong to
`ocr.py`. One bucket first, twenty documents first, because the failure this step can produce —
text nobody can read — is cheapest to catch at twenty.

The order is not the one you would guess, and the code enforces it. The legacy worker and the
bridge cannot both write the column: the worker's condition predates the bridge's write, so a
rename during the gap gets overwritten with stale text. So the queue is drained **before** the flag
goes up — and while it drains, the suite must still be running the legacy loops, because with the
loops off *and* the flag off nobody writes the column at all. `check` names that window
(`content writer: none (handover window)`), and Step 1.2 is inside it on purpose.

### 1.1 Drain the legacy `ocr` queue

**Do**

```bash
papra-suite jobs --kind ocr --drain
```

**Expect** the exit code to be the answer. While there is work it exits `1` and says so; when the
queue is empty it exits `0` and says `0 pending, 0 running` — and then tells you to read again a
minute later, which is not pedantry:

```
queue is drained: 0 pending, 0 running
  a row that has been claimed is not pending: to judge the handover, read this again a minute from now
  keep the delivery flag off until this command reports drained twice, a minute apart
```

**If not** — a count that is not falling between two reads is a queue that is not draining. Read it
(`papra-suite jobs --kind ocr --status failed --limit 20`, and `--status running` for a row that
has been claimed and never came back), repair or reset the rows (`papra-suite jobs --reset JOB_ID`),
and do not raise the flag until this exits `0`.

### 1.2 Turn the flag on, for one bucket

**Do** — in the `papra-suite` service on atlas:

- `PAPRA_INDEX_CONTENT_DELIVERY=true`, and **only** for the moment one bucket is under test
  (ATLAS — verify on the box: the Dokploy screen that holds the environment, whether the stack
  recreates the container or restarts it, and how long that takes)
- restart, then `papra-suite check`

**Expect**

```
  content writer: index bridge
```

**If not**

- `configuration error: PAPRA_INDEX_CONTENT_DELIVERY is on while PAPRA_LEGACY_WORKERS is also on`
  (exit `2`) — you raised the flag while the legacy loops were still enabled without proving the
  queue empty twice a minute apart. That is the two-writer refusal, and Section 2.4 explains why it
  is the feature. The fix is the order its own text names, not a different flag.
- `content writer: none (handover window)` — the flag did not reach the process. It is an
  environment variable read at startup, so: wrong screen, no recreate, or a typo. This state is not
  dangerous, it is simply not the state you think you are in.
- The restart drops documents mid-extraction. That is expected and recoverable: a claimed job dies
  with the process, and the sweep re-enqueues an un-delivered document. Do not treat a `running`
  row that vanishes as loss.

### 1.3 Backfill twenty, as a rehearsal first

**Do**

```bash
papra-suite backfill --org-id <bucket> --limit 20 --dry-run
```

**Expect** a plan and no queue writes: the scope, then the cost line — *"content already in
Kreuzakt: download, verify the hash, deliver — no VLM hour is spent"* for a reference that is
already bound, one Kreuzakt queue entry for a document that was never enrolled. `--limit` is what
stands between a rehearsal and the corpus; the real run refuses without it.

**If not** — a scope of `0` on a bucket that has documents means the flag or the org id is wrong,
and a backfill that queues nothing is not a pass. Fix that before going on.

**Do** — the same command with `--dry-run` removed, then:

```bash
papra-suite jobs --kind index --drain
papra-suite ledger --status
```

**Expect** `jobs --kind index --drain` to exit `0`, and `ledger --status` to show `stale: 0` for the
work you just queued. `papra-suite check`'s `index:` line carries `N delivered`, and it moves by
roughly the number you queued.

**If not** — a `delivery-blocked` count that grows is the index queue failing: `papra-suite jobs
--kind index --status failed --limit 20` names the documents and the transcript on each row carries
why. If Kreuzakt is down this step cannot pass; fix the connection (`PAPRA_KREUZAKT_URL`, Step 0.1)
rather than working around it.

### 1.4 Read those twenty in Papra, in German

**Do** — open the twenty in Papra's UI (ATLAS — verify on the box: the screen, and whether the
detail view shows the extracted `content` at all). For each, check the first screenful:

- German is German: not `????`, not `MÃ¼nchen`, not `M&#252;nchen`
- an amount reads the way the paper reads: `1.234,56`, not `1,234.56`
- `ß` survives: `Straße`, `Straße 12`, `Schlütersche`
- a word with an umlaut mid-line is not split by a stray space

**Expect** all four, on all twenty, and the same text from the index side once the listener exists
(Step 3.3).

**If not** — mojibake (`Ã¼`, `????`) is the extractor's output, not the delivery's: it is what
Kreuzakt returned. Record which documents and what you saw. **Do not go on to Step 2 with mojibake
in the corpus**: metadata delivery is independent of it, the rebuild that would fix the text
(Appendix A.2) is cheap, and a month of a human correcting badly-extracted text is not.

### 1.5 Include one digital PDF with a text layer, and look at its top band

**Why this is called out separately.** Two paths put text into the index, and they are not equally
faithful at the top of page 1.

- **The OCR path does not lose the top of the page.** Measured on the operator's own inference host,
  on a scan built for the purpose (text rendered to a bitmap, wrapped as a PDF, verified to carry no
  text layer): `Qwen3-VL-8B-Instruct-4bit` returned **all five lines, first line included**, in
  13 seconds — `Kfz-Haftpflichtversicherung`, `Versicherungsschein Nr. VH-2024-778812`,
  `Straßenbahnschienen und Grüße aus München`, `Rechnung über 1.234,56 Euro`,
  `IBAN DE89370400440532013000`. Umlauts and `ß` correct, the German decimal correct, the IBAN exact,
  and the auto-title sensible. Controller's measurement.
- **The text-layer path does.** On a hand-built canonical PDF (Helvetica, WinAnsi, one `BT/ET` block
  per line, no headers, no tables, one page), text placed at **y = 785, 770 and 755** on a
  **792-point** page is **absent from the extracted text**, while y = 735 and everything below
  survive; the bottom of the page is unaffected. Two marks were lost from `cupsfilter`-produced
  files and one from the hand-built file, so the loss is **positional and not ordinal** — roughly
  the **top 40–60 points**. It is not our client: the server's own `GET /api/documents/{id}` carries
  the same shortened `content`. It is not title consumption: titles come from filenames. It is not
  language: German extracts and searches correctly. Controller's measurement, on
  `ghcr.io/anaisbetts/kreuzakt@sha256:4a0109d0…`, with synthetic PDFs.

So the letterhead risk is real and it is **scoped**: **digital PDFs that carry a text layer can lose
their top 40–60 points — sender, document number, issue date — and scans do not.** Your archive is
mostly scans, which is the good news; the documents at risk are the ones that arrived as digital
PDFs (e-invoices, bank and insurer exports, PDFs printed from a browser, mail attachments that were
never paper). None of the 52 integration tests can see this either way: they assert the bridge, not
the extractor's fidelity.

**Do** — make sure the twenty in Step 1.3 include **at least one digital PDF with a text layer**
(`papra-suite jobs --document-id <id>` shows what ran; a text-layer document is one that never spent
OCR time). For that document, compare the two versions of its first lines:

1. Papra's UI, top of page 1, read with your eyes (ATLAS — verify on the box)
2. the index's `content` for the same document

and record, per document: `matches` / `top band missing` / `other`.

**Expect** either outcome to be *possible*. `matches` means that document's letterhead is indexed.
`top band missing` means the measurement above reproduces on a real document of yours, which is a
finding and not a malfunction.

**If not** — if the top band is missing, this is a decision, and this is the last cheap moment to
make it: accept it (and know that a search for an invoice number printed in the letterhead will come
back empty for that class of document), report it upstream, or pick the extraction path that does
not lose it. Do **not** find this out in Step 5, after `ocr.py` is deleted and the fallback that
could have re-read those pages is gone with it. If it does reproduce, note it in Step 3.4's evidence
table too: it bounds what "findable" means for that class.

### 1.6 `extraction-empty`, and how large is too large

**Do** — read the extraction-health numbers in `papra-suite check` against the corpus size in
`papra-suite ledger --status`.

**Expect** a number small relative to `current`. The build ships **no** threshold, so the rule is
written here in advance, as a ratio and not a constant, so that it is not invented later under
pressure: more than **1 in 20** delivered references coming back `extraction-empty` stops the
cutover. On ~500 documents that is 25; on the twenty you just delivered, **one** empty is worth
reading and **two** is a stop.

**If not** — an empty extraction is a document the extractor read and found nothing in: a scan whose
vision pass did not run (Step 0.3), or ran and said nothing. That is the signal the spec's §9 asks
for, and it is a reason to fix Step 0.3 / 0.4 and rebuild (Appendix A.2) — not to keep enrolling.
The whole argument for this cutover is that a scan becomes searchable; if `extraction-empty` is
normal here, the argument is not yet true.

### 1.7 Rollback is one flag — and what it does not undo

**Do** — set `PAPRA_INDEX_CONTENT_DELIVERY=false` **and** `PAPRA_LEGACY_WORKERS=true` in the
`papra-suite` environment, restart, re-read `papra-suite check`.

**Expect**

```
  content writer: suite ocr worker (legacy)
  metadata writer: suite title worker (legacy)
```

**Why two and not one.** The plan calls this a one-flag rollback, and in the sense that matters it
is: the bridge stops writing, and the code that used to write is still in the tree. But literally
turning only the delivery flag off while `PAPRA_LEGACY_WORKERS` is still `false` leaves you in the
handover window — `content writer: none (handover window)` — where the column has no writer at all. That is a fine
state for an afternoon and a wrong one for a weekend. The rollback is the pair, and `check` is what
tells you which of the three states you are in.

**And what it does not undo**: the `content` the bridge already wrote stays written. There is no
previous value to restore — Papra's own OCR text was overwritten, not versioned. The flag rolls
back the future, not the past. That is what Step 1.4, 1.5 and Step 4 exist for: they are the reason
the first twenty are read by a human before the next five hundred are written by a worker.

---

## Step 2 — metadata delivery on, and who owns each field

**Why.** The bridge's claim about Papra's `name`, `documentDate` and `notes` is not "the index wins"
but a rule with a memory: the bridge writes those fields **only for a document it enrolled itself,
and only until a human edits the field**. After Step 2 that rule is live on your archive, so this
step is mostly an audit of the rule's inputs — with one refusal in it that you must understand
rather than route around.

### 2.1 Drain the title queue, then raise the flag

**Do**

```bash
papra-suite jobs --kind title --drain
```

**Expect** the same shape as Step 1.1: exit `1` while anything is pending or running, exit `0` with
`0 pending, 0 running`, read again a minute later, and only then:

- `PAPRA_INDEX_METADATA_DELIVERY=true` in the `papra-suite` environment (ATLAS — verify on the box:
  the screen, and the restart behaviour)
- restart, then `papra-suite check`

**Expect** `metadata writer: index bridge`, and `content writer:` unchanged from Step 1.

**If not**

- `metadata writer: two writers (title queue draining)` — this combination is *reported and
  accepted*, not refused (Section 2.4 says why metadata and content differ here). It means the
  legacy loops are still on while the title queue empties: the producer stops enqueueing `title`
  jobs and `run_title_job` refuses the rows already queued, so the window is safe. It is not the
  end state; `papra-suite jobs --kind title --drain` is how you watch it close.
- `content writer: none (handover window)` after this restart — Step 1's flag did not survive the
  recreate. Two flags have to be present at once from here on; `check` prints both lines for a
  reason.

### 2.2 The ownership audit, read-only, before it writes anything wide

**Do** — with the flag on but before any large backfill, ask what the rule would do:

```bash
papra-suite metadata --restore-auto --dry-run
papra-suite metadata --dry-run
```

**Expect** the first to be a count and nothing else, e.g.

```
dry run: 0 name, 0 date, 0 notes in every organization [fields: name, date, notes]
```

and the second to print a verdict per reference. `--dry-run` changes nothing; both are safe to run
as often as you like.

**If not**

- `metadata --dry-run` refuses with exit `2` while the flag is off, naming the flag. That is intended
  (an audit of a rule that is not in force would describe a configuration you do not have), and it
  is why this sub-step sits after 2.1 and not before it.
- The restore counts are large: that is what 2.3 is for.

### 2.3 What a plausible number of `protected_name` looks like

**Do** — read `papra-suite ledger --status`'s protection line:

```
protected: 0 name, 0 date, 0 notes
```

**Expect** `name` near zero, `date` in the tens being normal (a human or Papra correcting a date is
an edit, and the bridge stops writing it), `notes` low. The rule this number reports is: **the
bridge wrote this field last, or a human did** — the flag flips to `human` the first time Papra's
value is not what the bridge wrote, and it stays there until you say otherwise. So:

- **`protected_name` near zero is the healthy reading** on an archive where nobody has renamed
  anything recently. If it climbs in proportion to the corpus on day one, the baseline is wrong: the
  bridge thinks every document was renamed by hand, which means it never recorded its own write —
  usually because the documents were enrolled before the ledger knew their original filename. That
  is a real state, and the repair is the audit in 2.2 plus `metadata --force` on a scope you have
  read first, one bucket at a time.
- **`protected_name` at 100 % and `indexed current` climbing** is the worst combination: the index
  is being maintained, titles are never corrected, and nothing looks wrong. This is the failure the
  README's "A human renamed it, and now the bridge will not" section is about. Read that section
  before you reach for `--force`.

**If not** — if you do conclude the baseline is wrong: `papra-suite metadata --field name --org-id
<bucket> --dry-run` first, read it, then the same command with `--force --yes-i-mean-it`. `--force`
without `--yes-i-mean-it` prints the scope and exits `1`; the two flags are the point. Do not run
`--force` over the whole archive to see what happens: it overwrites names a human may have typed,
and it is the one command in Step 2 whose output you cannot roll back with a flag.

### 2.4 The two-writer refusal: what it is, and what it is not

**Do** — nothing. This sub-step is for reading, because you will meet it, and it looks like a bug.

**Expect** this, if you raise the content flag while the legacy loops are still enabled and the
queue is not provably empty (exit `2`, from `check` and from `serve` alike):

```
configuration error: PAPRA_INDEX_CONTENT_DELIVERY is on while PAPRA_LEGACY_WORKERS is also on: this
suite's legacy OCR worker and this bridge would both write the `content` column, which is the race
this bridge exists to remove. In the required order: set PAPRA_LEGACY_WORKERS=false and recreate,
prove the queue is empty twice a minute apart (proves quiescence: `papra-suite jobs --kind ocr
--drain` prints both counts and exits 1 while anything is pending or running; two readings a minute
apart must both print 0), and only then turn on the delivery flag
PAPRA_INDEX_CONTENT_DELIVERY=true. While an ocr row remains, the convergence pass is fighting a
running job for the same document.
```

What that refusal **is**: the only mechanism available for ordering two writers that live in
different processes and never meet. The message is the runbook, in the required order, because the
person reading it at 23:00 is the person who just set the flag.

What it is **not**:

- not a bug, and not something to defeat by unsetting `PAPRA_LEGACY_WORKERS` *and* the flag "to get a
  clean start" — that is the handover window, where `content` has no writer and every new document
  arrives unread
- not applied to metadata: `metadata writer: two writers (title queue draining)` is a report, not a
  refusal, and the difference is deliberate — the legacy title chain is switched off from both ends
  while the flag is on (no new `title` jobs are enqueued, and rows already queued are skipped and
  marked done without writing), which is safe in a way that two writers of `content` is not
- not proof that your queue is dirty. It fires on the flags, not on the queue: you prove quiescence
  yourself, twice, a minute apart

**If not** — if you ever see the delivery flag on with the legacy loops on and *no* refusal, stop
and treat that as a code bug: the guard is the thing that makes Step 1 and Step 2 safe in either
order, and its absence is worse than the outage you are working through.

---

## Step 3 — finish the corpus, and prove the search this was all for

**Why.** Steps 1 and 2 proved the plumbing on twenty documents. Step 3 is where the archive becomes
searchable, and where the one claim that motivated the whole milestone gets measured rather than
asserted: that a scan of a German document, photographed and mailed, comes back from a search. Until
that has been seen on his own corpus, the cutover has proven only that text moves.

### 3.1 Finish the backfill, all buckets

**Do**

```bash
papra-suite backfill --dry-run
papra-suite backfill --limit 200
papra-suite jobs --kind index --drain
papra-suite ledger --status
```

**Expect** the rehearsal to print the whole scope with no org filter and cost it; then, in slices,
`stale` in `ledger --status` falling toward `0` while `jobs --kind index --drain` goes green between
slices. `unledgered` counts documents the ledger never held (nothing in ordinary intake reads the
whole corpus), so the finish line for the archive as a whole is **`stale: 0` *and* `unledgered: 0`
*and* a green index drain** — and the number to compare the slices against is the `unledgered` you
wrote down in Step 0.7.

**If not**

- A slice that queues nothing while `stale` stays put: read the scope the rehearsal printed. If it
  excludes what you expected, the eligibility is the answer — a document Papra reports deleted, a
  reference with no live row, or an oversized refusal (3.2). It is not a queue that is ignoring you.
- Rows in `failed` with the same error: fix that error and re-run the slice; the sweep re-enqueues
  anything un-delivered, so nothing is lost by waiting. If the error is Kreuzakt refusing, stop and
  go back to Step 0.3 — a backfill into a dead index is a way to spend VLM minutes for nothing.

**The clock you are actually buying** (measured 2026-11-20, three median-sized single-page scans from
the Uni Hamburg bucket, extracted on the operator's own inference host at
`Qwen3-VL-8B-Instruct-4bit`): **133 s** and **147 s** to extract, with a third document finishing only
after the 181-second window the probe had allowed. So budget **two to three minutes per page**, not
seconds — at 1,183 documents the corpus is a multi-day extraction, and the image's own README names
only `OCR_VLM_BASE_URL`, `OCR_VLM_MODEL`, `OCR_VLM_API_KEY`, `OCR_VLM_DEV_*` and
`OCR_VLM_TIMEOUT_SECS`, with no documented concurrency knob to turn. Two consequences: start the
full backfill only when a multi-day run is acceptable, and treat Step 3.2's `extraction-empty` count,
not a quiet-looking queue, as the authority on whether extraction is finished — because a document
whose extraction timed out is not distinguishable, over the API, from one still in flight. To watch
it without re-reading prose all day, poll the machine surface of the same readout:

```bash
watch -n 60 "papra-suite check --json | jq -c '{current: .index.indexed_current, empty: .index.extraction_empty, blocked: .index.delivery_blocked}'"
```

`extraction_empty` creeping up slowly is a throughput story; `extraction_empty` climbing fast while
`indexed_current` stalls is the extractor being wrong for this corpus, which is the case Step 3.2
names. Both numbers come from the one read that prints the lines above, so the two surfaces cannot
disagree about what to alert on.

**Spot-check one extraction against the paper while you are here.** One of the three scans, a
single-page certificate, came back with 10,191 characters of text. It may well be everything printed
on the page; it may also be the vision model expanding boilerplate into prose. Read the page and the
text side by side once. If the model inflates, the fix is a different `OCR_VLM_MODEL`, and you want
that decision made before 1,183 documents carry it, not after.

### 3.2 The documents that never arrived

**Why.** The ceiling from Step 0.6 kept some documents out of the archive entirely, and no backfill
can reach them: they are not un-indexed, they are not enrolled.

**Do** — read the `oversized:` line from `papra-suite check` (it reports what this process has seen
since it started, and which ceiling was in force), and compare the Step 0.7 baseline against
`ledger --status`'s totals to see how much of the archive the ledger has ever held.

**Expect** either a count with a largest-size and a since-date, or `none seen` — with the limit named
either way.

**If not** — for each document you want back, the remedy is on the human side: compress it and
re-upload to Papra, and it will be enrolled and indexed like any other. If the class is large rather
than a handful, raise `PAPRA_MAX_DOCUMENT_BYTES`, restart, and re-run `papra-suite backfill` — that
reaches what the earlier ceiling kept out of the *queue*, though never the mail attachments that were
refused at intake: those exist only in the mailbox log and the sender's outbox. Write down which
documents you decided to lose. That list is part of what "complete" means for this archive, and it is
the one gap in the index that no rebuild closes.

### 3.3 The search that has to work

**Why.** Everything above is infrastructure. This is the product: `search_documents` finding a German
scan by a German word, on the operator's own corpus.

**Do** — the listener has to be up to be asked, so enable it on loopback first (Step 6 does the
publishing; enabling early is harmless because the listener binds loopback only and nothing outside
this machine can reach it):

```bash
papra-suite mcp --enable --dry-run
papra-suite mcp --enable            # needs PAPRA_MCP_URL set: restart `serve` after
papra-suite mcp-token --create --label operator --all-buckets
papra-suite mcp --status
```

**Expect** the dry run to print `dry run: mcp_server_enabled: unset → 1` and change nothing; the real
run to print that transition, the note that `serve` reads these rows at start so **it must be
restarted**, and a note that no token exists yet; the token command to print the secret **once**
under `DO NOT LOSE THIS: …`; `--status` to read the rows back with `bind: 127.0.0.1:8765 (loopback)`.

**Do** — from the box, with the token in the environment and never on the command line:

```bash
export PAPRA_MCP_TOKEN=…      # the value printed once above
python3 docs/superpowers/probes/2026-11-20-tailscale-headers/mcp_call.py \
  --url http://127.0.0.1:8765/mcp --host papra-mcp.<your-tailnet-name> \
  --insecure-host search_documents query=Versicherungsschein page_size=5
```

`--host` is not decoration: the allow-list is built from `PAPRA_MCP_URL`, so a loopback call that
sends `Host: 127.0.0.1:8765` would be refused with `421` before the token was read. The probe prints
the session, the structured result, and one line per hit; it exits non-zero on a refusal.

**Expect** the document back, with `partial: false`. Then run the four queries in the table below —
they are the four behaviours that decide whether a "no results" answer means *absent* or *absent from
the index as it matches*, and they are what Step 6.4's agent-facing guidance is built on. All three
of the measured behaviours are controller's measurements on the operator's own endpoint:

| query | against a document containing | expected | why |
|---|---|---|---|
| `Versicherungsschein` | `Versicherungsschein Nr. VH-2024-778812` | **hit** | the word is present |
| `Versicherungsscheinen`, `Rechnungen` (doc holds `Rechnung`), `Grüßen` (doc holds `Grüße`) | any inflected form of a word in the document | **hit** | stemming works, and it is the point of the milestone; umlaut folding works too, so `grusse` finds `Grüße` |
| `Haftpflicht`, or `Straße` against `Straßenbahnschienen` | a **compound** whose component you typed | **no hit** | German compounds are **not** decomposed. Correct FTS behaviour, but "not found" here does **not** mean "not in my archive" |
| `strasse` against a document holding `Straße` | the `ß`/`ss` spelling difference | **no hit** | `ß` is **not** folded to `ss`, even though umlauts fold. A spelling difference is a real search miss |

Phrase queries work (`Versicherungsschein Nr.`, `Nr. VH-2024`). A `page_size` of 5 with
`scanned` far above it is normal: `scanned` is rows the index looked at, not candidates.

**If not**

- `index_unavailable` on every call: the listener refuses to guess. Read `mcp --status`, restart
  `serve`, and read `papra-suite check`. A refused search is the build behaving; an *answered* search
  that silently omitted the failed part is not — the answer would say `partial: true`, and if it does
  while you cannot see why, the `info` log line from Step 0.5 is where the reason lives.
- The compound rows behave as hits: also record that, and say so — it means matching is doing more
  than the measurement above found, and the guidance in Step 6.4 gets softer, not firmer.
- The inflected rows do **not** hit: stop. Do not go on to Step 4. That is the milestone's premise
  failing on the real corpus, and Step 4's whole argument for deleting the workers depends on it.

### 3.4 Record it, because spec §9 asks for a measurement and not a memory

**Do** — paste the four rows above, plus one row per document you tested, into a table here (or in
the file that will become the §9 evidence note), with the columns:

`date · document (org/id) · how it arrived (scan or digital) · query · expected · got · partial ·
top-band result from 1.5`

**Expect** four things in that table: the scan class proven searchable by an inflected German word;
the compound miss recorded as a miss; the `ß`/`ss` miss recorded; and the Step 1.5 top-band result
beside the documents it applies to. "It works" is not evidence; this table is, and it is the thing a
future reader will need when the extractor changes underneath it.

**If not** — if a cell is empty because you did not test it, leave the cell empty and say so. A §9
claim that quietly covers untested ground is how this milestone gets re-litigated a year from now.

---

## Step 4 — prove Kreuzakt can rebuild one document, from Papra's bytes, on its own

**Why.** Step 5 deletes the suite's own OCR worker. The justification is not "the index is good" but
"the index can rebuild a document from the archive without the worker". That is a claim about a
path, and paths are proven by walking them once, on a document, while the fallback still exists. If
this drill fails, **deleting the workers is off the table**, full stop — and the drill is cheap.

The shipped path that forces a genuine re-extraction of a single document is the restore path: the
deletion pass propagates a Papra delete to the index, and a restore in Papra **drops the binding
unconditionally and re-admits the index job** — because the code refuses to assume the index copy
survived. So the drill makes Kreuzakt forget one document and then asks it to make the document
again from bytes it never produced.

### 4.1 The drill

**Do**

1. Pick a document you can afford to put in the trash for ten minutes, and remember it by
   `org`/`document id`. Not a unique document: one you have a copy of.
2. `papra-suite jobs --document-id <id>` — note the settled `index` row's job id. This is also the
   command that answers "did this document get indexed, when, and how many tries".
3. Trash it in Papra (ATLAS — verify on the box: the screen, and whether Papra's trash is
   restorable in the version deployed — check this on a throwaway document first, and write down
   what you saw).
4. Wait for one deletion pass, and read it in `papra-suite check`:

   ```bash
   papra-suite check
   ```

5. Restore it in Papra.
6. Wait for the next pass, then read the queue and the ledger:

   ```bash
   papra-suite jobs --document-id <id>
   papra-suite ledger --status
   papra-suite jobs --kind index --drain
   ```

**Expect**, in order:

- the `deletions:` line moving — a document marked, and the pass reporting what it did. Passes run
  from the sweep loop and need Kreuzakt up; the line also names the reasons a pass refuses, and
  `never ran` after two sweep intervals means it has not been able to try.
- while the document is trashed and marked, `get_document` on its id answers `not-found`. That is the
  archive's own no, asked before the index is touched.
- after the restore: the binding dropped, the index job re-admitted, a fresh `index` row that ends
  `done`, and `ledger --status` showing that reference current again. The log for that job says the
  index document was gone behind the binding and was re-uploaded and rebound in place.
- Papra's `content` for that document full again, the auto-title converged, and a search for the word
  you recorded in Step 3.4 finding it.

**If not**

- `deletions:` still `never ran`, or a pass that reports nothing after both the trash and the
  restore: the listing walk is not fresh or Kreuzakt was not up when the pass ran (the same line
  says which). Fix that and wait another pass; the drill is not failing, it is not running.
- The row re-appears as `failed`: `papra-suite jobs --reset JOB_ID` and read the transcript. A
  re-extraction that fails here, with the legacy worker still in the tree, is the finding you came
  for — write down the error before you reset anything.
- The document comes back in Papra but **not** in search: stop. Do not go to Step 5. The rebuild path
  is what makes the deletion safe, and until it works the fallback stays.
- You cannot make Papra trash and restore cleanly (ATLAS — verify on the box): then the equivalent
  drill is to delete that one document inside Kreuzakt on the private network and `papra-suite jobs
  --reset` the index row, which takes the same 404-behind-the-binding path. Write down which variant
  you ran.

### 4.2 What this step buys, in one sentence

If Kreuzakt can rebuild a document from Papra's bytes while `ocr.py` is still in the tree, then
deleting `ocr.py` removes a duplicate extractor and not the last one. If it cannot, the deletion is
not a cleanup, it is a single point of failure — and this is the last step where finding that out
costs nothing.

---

## Step 5 — delete the workers

**Where this happens: on the laptop, with the controller — not on atlas.** This is a source change
in a git worktree, reviewed and tested before it is deployed. Nothing in Step 5 is typed on the
server; the server sees it later, as a deployment (Step 5.4). If you are on `atlas` reading this
step, go back to Step 4 and finish the drill first: the drill is what earns the deletion.

**Why now and not earlier.** Steps 1–4 all keep the fallback alive on purpose: if the index is wrong,
the old extractor is still there. Step 5 is the one step that removes an option, which is why it sits
after the re-extraction drill and not after the backfill.

### 5.1 What "no references remain" actually means here

**Do** — from the worktree, run the plan's greps and read them as a *work list*, not a verdict:

```bash
grep -rn 'run_ocr_job\|run_title_job\|enqueue_ocr_job\|enqueue_title_job\|OCR_MAX_PAGES\|ocr_dpi' \
  papra_suite tests README.md
grep -rn 'kind="ocr"\|kind="title"' papra_suite tests README.md
```

**Expect** — and this is a correction to the plan, which says these come back empty once
`ocr.py`, `titles.py`, `test_ocr.py` and `test_titles.py` are deleted. Measured on this tree, before
any deletion:

- the first grep: **87 matches** across `papra_suite/config.py`, `papra_suite/mail.py`,
  `papra_suite/pipeline.py`, `README.md`, `tests/integration/test_ocr_pipeline.py`,
  `tests/test_config.py`, `tests/test_mail_poller.py`, `tests/test_metadata_delivery.py`,
  `tests/test_pipeline.py`, `tests/test_workers.py`. Four files deleted is not eight files edited.
  The plan's list of files to remove is incomplete by at least
  `tests/integration/test_ocr_pipeline.py`, and its "Expected: no matches" is only reachable after
  edits to the six test modules and the three source modules that reference the workers.
- the second grep: **84 matches**, and **most of them must survive.** `ocr` and `title` remain legal
  kind labels forever: rows already in `jobs` keep them, `papra-suite jobs --kind ocr` has to keep
  working to read history, and the tests that prove that are load-bearing. The thing that must reach
  zero is the **producer** side — nothing may *enqueue* those kinds any more. So the honest end state
  is: no `enqueue_ocr_job`/`enqueue_title_job` call sites, no `run_ocr_job`/`run_title_job`, and
  `kind="ocr"` still appearing in readers, help text, tests and the README's queue section.

**If not** — if you find yourself deleting a test because it references a kind label rather than a
worker, stop: that test is protecting the historical rows. Rewrite it, do not remove it.

### 5.2 `llm_base_url` and `llm_api_key` stay, and here is why

**Expect**, after the deletion, that both config fields are still in the tree:

- `llm_api_key` is registered as a **secret to redact** (`register_config_secrets` adds
  `config.llm_api_key`, `config.papra_api_key` and `config.admin_password` to the redactor). Deleting
  the workers does not make the key less secret; it removes the code that used it, and the redaction
  is the only remaining reference.
- `llm_base_url` and `llm_api_key` are **required** environment variables in `load_config`
  (`_require` / `_require_base_url`), alongside `PAPRA_BASE_URL`, `PAPRA_API_KEY`, `TITLE_MODEL`,
  `OCR_MODEL` and `ADMIN_PASSWORD`. Removing them from the config schema is a breaking change to
  every deployment's environment, and `check`/`serve` would refuse to start on atlas the moment the
  deployment disagreed with the schema.

So the plan's condition — "keep them only if something still uses them" — resolves to: **keep both,
in this commit**, and if the fields become dead weight, retire them as their own change with the
README's variable table and the deployment environment moved in the same commit. What you may delete
here is the *worker* code, the worker tuning knobs that nothing reads any more, and their tests.

**If not** — if you do delete a required variable, the failure is not subtle: every `papra-suite`
command in this runbook exits `2` with `configuration error: Missing required environment variable:
…`. That includes `check`, which is the command Step 0 tells him to run first.

### 5.3 The live `ocr`/`title` rows are inert. Do not delete them.

**Do** — nothing. Write this down instead, in the commit message and in the README's queue section.

**Expect** documents already in Papra keep their text, references keep their bindings, and rows in
`jobs` with kind `ocr` or `title` stay exactly where they are: `done`, `failed`, whatever they were.
They are history, not work. Deleting them as part of the code change would destroy the only record of
what the old worker did to which document, and `papra-suite jobs --document-id …` is the command the
README names for "did this document get its text, and when". A cleanup of old settled rows is a
different decision, with its own argument, and it is not part of a deletion.

**If not** — if the deployed queue still has `pending` or `running` `ocr`/`title` rows at deploy time
(Step 0.7 baseline, Step 1.1, Step 2.1 are how you get there), the new build simply has no worker for
them: they sit there, and `papra-suite jobs --kind ocr --drain` will never go green. That is not
corruption and it is not a reason to delete rows: it is the reason the flags move **before** the code
does, and the reason Step 1.1 and Step 2.1 exist.

### 5.4 The commit, and what it has to say

**Do** — on the laptop, with the controller:

```bash
make lint
make test-unit
git status --short          # explicit paths only; never `git add -A`
git add papra_suite/ocr.py papra_suite/titles.py tests/test_ocr.py tests/test_titles.py <the edited files>
git commit
```

**Expect** `make lint` to pass and `make test-unit` to pass. Record the numbers, because the commit
message must carry the delta: today the unit gate is **1502 passed, 52 deselected**, and the tests
that go with the workers are **8 in `tests/test_ocr.py`, 12 in `tests/test_titles.py`, 2 in
`tests/integration/test_ocr_pipeline.py`** (controller's collection counts). The commit message names
the integration-test count delta explicitly — which files were removed and what the integration
count went from and to — because the integration suite is the one gate that cannot be run here, and
a reader three months from now needs to know that 52 became 50 on purpose.

**Expect** the integration count itself to be measured on atlas (`make test-integration` — ATLAS —
verify on the box: the README says the whole command has to execute on atlas, and an
`ssh atlas && make test-integration` that runs `make` on the laptop is not it). A green unit gate is
not a green suite.

**If not**

- The unit count drops by more than the deleted files' tests: something else lost coverage. Find it
  before committing; a deletion that silently deletes an assertion is how a cutover loses its safety
  net without anyone noticing.
- `make test-integration` cannot run because Papra's stack has no volume (Step 0.2): then say so in
  the commit message and in the handoff, in those words. Do not describe the change as integration
  tested when it was not.
- The deployment after this commit shows `content writer: index bridge` and `metadata writer: index
  bridge` with no legacy lines to fall back to. That is the point of the step, and it is why Appendix
  A.3 now lists one more thing under *irreversible*.

---

## Step 6 — open the MCP listener to the tailnet

**Why.** This is the only part of the build that answers the network, and it is the only step whose
mistake is visible to something other than you. Everything before it moved text inside one stack;
this hands a read-only view of the archive to `pi` and `hermes`, each scoped to the buckets its token
names.

**One correction to "the only part", because it is the easiest way to expose the archive by accident.**
Kreuzakt also answers HTTP, and it answers it with a human web UI: `GET /` on the container returns a
35 KB page titled *Kreuzakt Document Search*, a Next.js front end over the same API this suite calls.
It has **no authentication of any kind** — measured on the pinned image, `GET /api/documents` with no
credential answers `200` with the corpus. Inside the stack network that is deliberate, and it is why
`papra-suite` can talk to the index without a second secret. The consequence for this step is narrow
and firm: the thing that gets a tailnet domain is the MCP listener, never Kreuzakt. Publishing its
port, or giving it a `tailscale serve` front, would put every document — and the ability to upload,
edit and delete them — in reach of anything that can route to it, and it would bypass the bearer
tokens, the bucket scoping and the Papra liveness checks in one move, because all three live in our
listener and none of them live in Kreuzakt. Our test compose publishes it on `127.0.0.1:12400` for the
sake of the tests; the production snippet in the README publishes no port at all.

Its legitimate use is as a debug surface, from the box. It is the quickest way to read one document's
extracted text beside the page it came from, which is the spot-check Step 3.1 asks for — run the read
from a container that has `curl`, since the Kreuzakt image has none.

The listener is loopback-only unless you say otherwise in the database, and it refuses every request
whose `Host` or `Origin` is not derived from `PAPRA_MCP_URL` — **before** authentication. That is
what makes Step 0b a prerequisite rather than a footnote.

### 6.1 Enable it, on the box

**Do** (if Step 3.3 already enabled it, this is a re-read; say which)

```bash
papra-suite mcp --status
papra-suite mcp --enable --dry-run
papra-suite mcp --enable
papra-suite mcp --status
```

then restart `serve` (ATLAS — verify on the box: how the stack restarts it).

**Expect** `--status` to report `mcp_server_enabled: 1`,
`mcp_bind_nonloopback: unset (effective 0 — only loopback may be bound)`,
`bind: 127.0.0.1:8765 (loopback)`, `public url: https://… (PAPRA_MCP_URL)`, and the sentence that a
running `serve` keeps the listener it already bound, **so the change needs a restart**.

**If not**

- `mcp refused: the listener would be enabled, and this is not a configuration \`serve\` starts` /
  `PAPRA_MCP_URL is required` (exit `2`, nothing written) — the URL is missing. It is not a formality:
  the Host and Origin allow-lists are built from it, and without it every proxied request would be
  rejected with `421`. Set it to the exact public URL from Step 0b and run the command again.
- The flags were already set and a listener is *already running* with the old settings: restart
  `serve`. A running listener keeps its port and its rows until that process stops — which is also
  the rollback in 6.5.

### 6.2 One token per machine

**Do**

```bash
papra-suite mcp-token --create --label pi --org-id <bucket-for-pi>
papra-suite mcp-token --create --label hermes --org-id <bucket-for-hermes>
papra-suite mcp-token --list
```

**Expect** each `--create` to print the secret exactly once, headed
`DO NOT LOSE THIS: the token for 'pi' (…) is printed once and cannot be shown again`, and the next
line to say it cannot be recovered — put it into the client now. `--list` shows
`label · buckets · created · last used · revoked`, with `last used: never` until something calls.
Never put a token in a shell command line, a URL, or this file: it is a bearer credential, and the
command that mints it deliberately cannot print an existing one.

**Expect** the scoping to be the decision, not the label: `--org-id` may be repeated for a machine
that legitimately reads several buckets, and `--all-buckets` is for the operator's own token — the
one you used in Step 3.3. A machine that needs one bucket gets one bucket. The hard edge is on
scope, not on count: a token may name **at most 8 buckets** (`--create` refuses the ninth), and a
label is at most 64 characters.

**If not**

- A token is lost: `papra-suite mcp-token --revoke --label NAME`, mint a new one. There is no
  recovery and there should not be.
- A token was leaked or a machine is decommissioned: revoke the same way; `check`'s
  `tokens: N active, M revoked` is the readout, and revocation is the only answer this build has —
  there is no expiry to wait for.
- You are tempted to give an agent `--all-buckets` "to get it working": don't. That flag is what Step
  6.4 exists to prove you did not need.

### 6.3 Publish, using the table from Step 0b

**Do** — point `tailscale serve` at `127.0.0.1:8765` (or whatever `PAPRA_MCP_PORT` says), then set
`PAPRA_MCP_URL` to exactly the public URL that serves it, and restart `serve` so the allow-list is
rebuilt from that URL (ATLAS — verify on the box: the exact `tailscale serve` configuration, and
whether TLS is terminated by serve or by you).

**Expect**, read against your own Step 0b table:

- If the measured `Host` was the public tailnet name → `PAPRA_MCP_URL=https://<name>.ts.net` is
  correct as it stands.
- If the measured `Host` was `127.0.0.1:8765` or anything else the proxy invented → the allow-list
  built from the public URL will reject every real request with `421`, and no token will help. Either
  make the proxy forward the original Host, or put the measured value where the allow-list will see
  it. Do not solve this by disabling the check; there is nothing to disable — the check is the
  rebinding defence.
- If a hand-set `Origin` did not survive the proxy → MCP clients that send one will get `400`. That
  is a proxy fix, and it is the one symptom that will otherwise look like a broken token.
- `X-Forwarded-*` arriving from a non-loopback peer → note it. The listener trusts proxy headers from
  loopback only, so what matters is what the socket says, and the rate limiter counts per client
  address from that.

**Expect** nothing here to be guesswork: every "if" above is answered by a row you filled in during
Step 0b, in the topology you are actually deploying. If the table is empty, stop and go measure —
this is the step where a laptop cannot help you.

**If not** — if the listener answers `421` from the tailnet, you have a Host mismatch; `401` means
the token did not arrive or is revoked; `400` on POST means no `Origin`. Those three codes are the
whole diagnosis at the proxy boundary, and `papra-suite mcp --status` is what says what the listener
believes its public URL is.

### 6.4 Verify the boundary with a scoped token, from a machine

**Why.** After the oracle collapse, the answer carries no scope information at all. So the test is
not "does it say *not-in-scope*" — **there is no such code, and there never will be**: the reason an
id is refused is deliberately not on the wire, because it would name buckets. The test is that a
scoped client learns *that* other buckets match, and never *what* they hold.

**Do** — from another tailnet machine, with `pi`'s scoped token:

```bash
python3 docs/superpowers/probes/2026-11-20-tailscale-headers/mcp_call.py \
  --url https://<name>.ts.net/mcp --token-env PAPRA_MCP_TOKEN \
  search_documents query=<a word that exists only in a bucket pi cannot see>
```

and then the same call with the operator's `--all-buckets` token.

**Expect** — this is the controller's measurement of exactly this pair against a real listener with a
two-bucket store, scoped token first:

```json
{ "items": [], "page": 1, "page_size": 20, "partial": false, "scanned": 1,
  "stats": { "kept": 0, "dropped_unauthorized": "1", "dropped_unmatched": "0",
             "dropped_deleted": 0, "dropped_lookup_error": 0, "dropped_unreached": 0,
             "expanded": 0 },
  "buckets_reached": [], "buckets_skipped": [], "partial_reason": null }
```

- **zero items**, `partial: false`, and the word `not-in-scope` **nowhere in the response**
- `dropped_unauthorized` present, as a **band** (`"1"`, `"2-9"`, `"10+"`), never an exact count for a
  scoped token — that band is the accepted disclosure: `pi` learns *that* something matched
  elsewhere, which is a fact it could infer from a Papra search anyway
- the same call with the all-buckets token returning the document itself, with its real `org_id`
- `get_document` on an id from a bucket the token cannot see answering **`not-found`**, and the
  string `bucket` appearing nowhere in that answer. The real reason is in the `info` log only, where
  it names the holding bucket and the token's label — which is why Step 0.5 mattered.

**If not**

- **Any content from a bucket the token is not granted** — a title, a date, a filename, a snippet —
  is a containment failure. Stop: `papra-suite mcp --disable`, restart `serve`, and the port is gone
  (`mcp: disabled (nothing listens…)` in `check`). Then write down which call produced it, with the
  token's label, before anything is re-enabled. Do not "retry to confirm".
- `dropped_unauthorized` came back as an exact count for a scoped token: also stop and report — the
  band is the containment, and an exact number is a changed disclosure policy.
- `421`/`400`/`401` instead of an answer: that is Step 6.3's table, not a scope leak. Fix the
  boundary and re-run; a request that never authenticated proves nothing either way.
- `partial: true`: the search did not see everything it should have, and `partial_reason` says which
  buckets were not reached. `pi` is told to treat that as "do not conclude absence", so it is safe —
  but find out why before you call the cutover done.

### 6.5 Write the boundary down for whoever builds the client

**Do** — hand the client's author these four lines, which are the whole operator-facing contract:

- A hit is the whole claim. Every hit was authorized by a ledger reference, and Papra said the
  document is not in the trash.
- A document you cannot see answers `not-found`. Nothing distinguishes it from a document that does
  not exist, and nothing will: the distinction is in the server log, on purpose.
- A search that returns nothing may be genuinely empty. If `partial` is true, it is not: read
  `partial_reason` and retry later — do not conclude absence.
- **A word that is part of a longer German word is not a search.** `Haftpflicht` will not find
  `Kfz-Haftpflichtversicherung`, and `Straße` will not find `Straßenbahnschienen`. Inflection works
  (`Rechnungen` finds `Rechnung`, `grusse` finds `Grüße`), but `ß` does not fold to `ss`, so `strasse`
  misses `Straße`. Before an agent tells a human something is not in the archive, it has tried the
  compound, and it has tried the other spelling.

**Expect** the agent's own instructions to carry that, because an agent that does not know it will
report "not in your archive" about documents that are in it, and that is the failure mode a user
will blame the cutover for.

**If not** — if the client treats `not-found` as proof of absence, or treats an empty search as
proof of absence, fix the client before you point it at the archive. The server cannot make that
claim correct for it.

---

## Appendix A — rollback, rebuild, what is unrecoverable, and the command list

### A.1 The switches, in one table

| What you want to stop | Set | Restart? | What `check` says afterwards |
|---|---|---|---|
| the bridge writing `content` | `PAPRA_INDEX_CONTENT_DELIVERY=false` **and** `PAPRA_LEGACY_WORKERS=true` | yes | `content writer: suite ocr worker (legacy)` |
| the bridge writing `name`/`documentDate`/`notes` | `PAPRA_INDEX_METADATA_DELIVERY=false` (leave `PAPRA_LEGACY_WORKERS=true` if the title worker must run) | yes | `metadata writer: suite title worker (legacy)` |
| everything back to pre-cutover | all three delivery flags `false`, `PAPRA_LEGACY_WORKERS=true` | yes | both writers named `legacy` |
| no writer at all (a quarantine, not a state to live in) | delivery flags `false`, `PAPRA_LEGACY_WORKERS=false` | yes | `content writer: none (handover window)` |
| the index entirely | `PAPRA_KREUZAKT_URL` unset | yes | search falls back to Papra; the ledger stops being consulted |
| the network door | `papra-suite mcp --disable` | yes (`serve`) | `mcp: disabled (nothing listens…)` |

Two things this table is claiming, said plainly:

- **Flag rollbacks stop the future.** Nothing here restores a Papra value the bridge overwrote. The
  record of what it wrote is in the ledger and the job transcripts, not in a previous version of the
  row.
- **After Step 5 there is no `legacy` row to go back to.** The flags still exist and `check` still
  prints them, but the code behind `PAPRA_LEGACY_WORKERS=true` has been deleted from the deployed
  tree. From Step 5 on, the rollback for a wrong extraction is Kreuzakt plus a rebuild (A.2), or a
  `git revert` and a redeploy — not a flag.

### A.2 Rebuilding by generation

**When.** Kreuzakt's `/data` was wiped or restored from a different backup, its ids are known to be
wrong, the extractor changed in a way that invalidates what is indexed, or you decided (Step 1.5)
that a whole class of documents must be read again.

```bash
papra-suite ledger --status                      # what is there now
papra-suite ledger --reindex-all --dry-run       # scope, no writes, no health probe
papra-suite ledger --reindex-all --yes-i-mean-it
papra-suite ledger --status                      # while it drains
papra-suite jobs --kind index --drain
```

**What it does.** Refuses unless Kreuzakt's `GET /api/health` says `status: ok` (exit `2`, generation
untouched); bumps the generation once, so the whole corpus is stale at once; then queues documents
through the same intake the sweep uses, paging by keyset, resumable. `ctrl-C` prints
`ledger stopped: interrupted.` and exits `1`; finishing it is `papra-suite ledger --requeue
--yes-i-mean-it`, which queues without bumping — because re-running the invalidation to finish a
partial rebuild would throw away the bindings the first pass had already re-derived.

**The finish line** is `stale: 0` in `ledger --status` **and** a green `jobs --kind index --drain`.
Do not use `current == references`: `references` includes rows the index will never hold, so that
condition can stay unreachable on a healthy corpus.

**It does not refill the legacy `ocr` queue**, and after Step 5 there is no legacy queue to refill.
If the point of the rebuild is to re-extract rather than re-bind, say so out loud before you run it:
this command re-verifies bytes against hashes the ledger already holds, and for unledgered documents
it reads Papra for the hash. It never writes to Papra — if it changes a byte of a Papra record, that
is a bug, and it is worth a halt.

Related, and quieter than they look:

```bash
papra-suite ledger --cleanup --yes-i-mean-it     # settle queued upstream deletes
papra-suite ledger --invalidate --yes-i-mean-it  # the older name for --reindex-all
papra-suite ledger --clear-marks --yes-i-mean-it # forget marks from a run you have fixed
```

### A.3 Disposable, recoverable, not reversible

| | |
|---|---|
| **Disposable** | `kreuzakt-data` — Kreuzakt's copy of each original and its own queue. Losing it costs extraction time and nothing else: the documents are Papra's, the bindings are the ledger's, and A.2 rebuilds the rest. |
| **Recoverable, with work** | `state.db` — README, *`state.db` is lost or corrupt*. You lose the ledger's bindings, the seen table, and every MCP token hash, and you rebuild with `ledger --reindex-all`. Every agent token stops working and has to be minted again: that is the correct behaviour for a lost secret store, and it is the reason tokens are hashed at rest. |
| **Recoverable, with a backup** | Papra's database. It is the archive: documents, names, dates, notes, and the bytes. Nothing in this runbook backs it up for you, and Step 0.2 exists because a stack can be deployed with Papra's data in the container filesystem — the test compose in this repo does exactly that, and `down` deletes it. |
| **Not reversible by any flag** | Papra `content`/`name`/`documentDate`/`notes` values the bridge overwrote. A human edit is protected from then on and stays protected until `metadata --force` on a scope you have read first. |
| **Not reversible by any means** | Documents the size ceiling refused at the mailbox and that nobody re-uploaded. They are not in the archive, so no rebuild can index them. Step 3.2 is where you decide which ones you are willing to lose. |
| **Not reversible by a flag, reversible by git** | Step 5. The tree keeps the history; the deployment does not keep the worker. |
| **Not reversible, by design** | A token that was printed once and lost; a token that was revoked. Mint or re-mint. There is no recovery endpoint and there should not be. |

### A.4 The commands this runbook uses

Every line below is the captured `usage:` of this build (`python -m papra_suite <sub> --help`, taken
from the branch this runbook was written on; the full capture is in
`.superpowers/sdd/2026-11-17-kreuzakt-index-bridge/task-16-runbook-report.md`). If your `--help`
disagrees, your build is different, and this file is stale for that build.

```
usage: papra-suite check [-h]
usage: papra-suite serve [-h]
usage: papra-suite replay [-h] --mailbox-id MAILBOX_ID [--from-uid FROM_UID] …
usage: papra-suite events [-h] [--limit LIMIT] [--mailbox-id MAILBOX_ID] …
usage: papra-suite metadata [-h] [--restore-auto | --force] [--org-id ORG_ID] …
usage: papra-suite ledger [-h] …
usage: papra-suite mcp-token [-h] (--create | --list | --revoke) …
usage: papra-suite jobs [-h] [--reset JOB_ID | --drain] [--kind KIND] …
usage: papra-suite backfill [-h] [--org-id ORG_ID] [--limit LIMIT] [--dry-run] …
usage: papra-suite mcp [-h] [--status | --enable | --disable] …
```

Ten subcommands: `check`, `serve`, `replay`, `events`, `metadata`, `ledger`, `mcp-token`, `jobs`,
`backfill`, `mcp`. This runbook uses eight of them; `replay` and `events` belong to the mailbox side
of the suite and nothing in the cutover needs them.

### A.5 What this runbook does not cover

- **The dashboard.** `serve`'s read-only dashboard is not part of the cutover and is not evidence for
  it; every claim here is a command or a log line.
- **Papra's own configuration** beyond `DOCUMENTS_OCR_LANGUAGES` and the size ceiling: mailbox setup,
  allow-list, retention. The README covers those.
- **`tailscale serve` syntax.** Step 0b measures what your proxy does; it deliberately does not
  prescribe flags, because the correct ones depend on a version and a topology this file cannot see.
- **`ocr.py`'s internals after deletion.** Once it is gone, the way to change extraction is Kreuzakt
  and its model configuration, not this tree.
- **Anything the file says `ATLAS — verify on the box`.** Those are the places where a laptop wrote a
  question instead of an answer. They are listed together in the runbook's report so nobody has to
  hunt for them, and a filled-in copy of this file is the artifact the cutover leaves behind.
