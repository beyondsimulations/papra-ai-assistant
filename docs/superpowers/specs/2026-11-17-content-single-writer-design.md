# Single writer for `content`: turn Papra's extraction off, and close the hole that exposes

Date: 2026-11-17
Status: proposed, awaiting operator decisions named in §9 and §10
Branches: written on `simplify-ingest`; assumes `ingest-visibility` merges first (§11)
Evidence: `docs/superpowers/probes/2026-11-16-custom-http/` (probes A, A2, B, B2, C2, E)
Supersedes: the "content-write race is undesigned" state left by the addendum in
`2026-11-16-ocr-content-search-intermittent-characterisation-design.md`

## 1. The problem, with the numbers on it

Two processes write the same column. Papra runs its own content extraction when a document
arrives and writes the result into `content`; our pipeline OCRs locally and writes the same
column with `PATCH`. There is no extraction-status field to wait on and no locking — the last
writer wins, and the loser is silently gone.

Measured (`probe_c2_exact.py`, two containers differing in one environment variable, same
image-only scan, `PATCH` immediately after upload, exact-text comparison, our text carrying a
token that exists on no pixel of the page):

| | our text still stored at t+2 … t+90 s | `Straße` findable |
| --- | --- | --- |
| Papra extraction on | **gone from t+2 s onward** | miss |
| `DOCUMENTS_CONTENT_EXTRACTION_ENABLED=false` | **byte-exact at all 9 samples** | hit |

The timing is what makes it intermittent. The OCR worker's `PATCH` normally lands after
Papra's extraction has finished — Papra's text on a 200 dpi scan was detected stable within
3.1 s — and then nothing overwrites it. On a big scan, a loaded OCR worker, or a restarted
suite, the `PATCH` lands first and Papra's extraction erases it.

The competing writer is also worse for this corpus, which is why "let it win sometimes" is not
a neutral outcome. Papra's default strategy is real OCR (`/app/eng.traineddata` ships in the
image) but it is configured with English language data only. A page printed
`Die Wäsche Straße Grüße Café Köln` was stored as `Die Wasche StraBe GriiBe Café Kdln`, and
content search then returned **miss** for `Straße`, **miss** for `Grüße`, **miss** for `Köln`.
Papra folds umlauts at query time (`Wäsche` finds `Wasche`) but does not fold `ß` and does not
expand `ae`/`ue`, so those misses are not recoverable by typing the query differently. The `ß`
mangle was not even reproducible — `StraBe`, `StraB3e`, `StraBBe` across three runs of the same
fixture — so no search-time substitution can compensate.

## 2. Decision

**Set `DOCUMENTS_CONTENT_EXTRACTION_ENABLED=false` and make our pipeline the only writer of
`content`.**

It is an upstream environment variable read by the shipped image — not a fork, not a patch — so
it survives pulling Papra forward, which was a stated constraint. It removes the second writer
instead of mitigating the race, so there is no window to lose, no ordering to infer, and no
sleep to get wrong.

The alternatives, and why they lost:

- **`custom-http`** (Papra calls our endpoint to extract). Measured (`probe_b_custom_http.py`,
  `probe_b2_retry.py`): extraction is asynchronous, so an upload returns in 0.1 s and a slow
  local VLM never delays intake; Papra POSTs `multipart/form-data` with the file in the `file`
  field; a plain-text response body is stored verbatim. That is a good contract. What kills it
  as the default is the failure mode: over a full 180 s window we received **exactly one**
  request on HTTP 500 and **exactly one** when we answered 12 s past the 5 s timeout, the late
  answer was **discarded**, and the document's `content` stayed empty with nothing on the
  document saying so. With a local model, "slow" is ordinary operation, not a corner case. It
  also requires an inbound listener — reachable from the container, authenticated only by
  whatever we put in `CONTENT_EXTRACTION_CUSTOM_HTTP_HEADERS`, because Papra sends no
  credential of its own. Keep it as the documented fallback for §10's non-goal (documents that
  reach Papra without passing through us).
- **Bounded reconciliation** (keep both writers, re-`PATCH` until our text sticks). It accepts a
  writer measured to land ~2 s after upload and win, and every variant of it needs either a
  distributed lock or an optimistic-concurrency signal that Papra does not expose.
- **Query-side workarounds** (search differently, tag documents, accept Papra's text). Rejected
  in the earlier characterisation spec and unchanged here: they cannot recover letters that were
  never stored.

## 3. What the decision creates, precisely

With extraction off, Papra never writes `content` at all (measured: `QRZ7IMAGEONLY`, printed on
the page, never became findable in that arm). Our text is then the only text. That is the point
of the change — and it removes the accidental safety net that today hides our own recovery gap.

The gap, read out of `papra_suite/pipeline.py` and `papra_suite/store.py`:

- `enqueue_ocr_job` deliberately enqueues **before** recording the document as seen, so a crash
  in that window is covered: the sweep re-discovers the document.
- A job claimed by a worker that then died is rewound to `pending` at startup.
- `worker_loop` fails a job permanently on the first attempt for any 4xx except 429, or for
  `error.permanent` (a completion whose `finish_reason` says it stopped early), and otherwise
  backs off and gives up after 3 attempts. The row ends `status='failed'`.
- **`sweep_once` skips documents already in the `documents` table**, and that row is written at
  intake — long before any text exists. So a document whose OCR job ends `failed` has no text,
  and nothing in the process will ever look at it again.
- `Store.reset_job` exists and is the correct primitive (see §5.2 for why an insert cannot
  work), and **has no caller anywhere in the product**. There is no `jobs` subcommand on this
  branch; `papra-suite` offers only `check`, `serve`, `replay`. So today the operator cannot
  even repair such a document by hand.

Two things I want on the record because I had them wrong once:

- A 409 `document.already_exists` from upload does not enqueue OCR, but it is **not** a gap:
  that path writes no `documents` row either, so the sweep re-discovers the document on a later
  pass. (I earlier described 409-on-replay as unrecoverable; it is not.)
- The gap is not "content might be lost". With extraction off it is exactly: **seen document +
  ocr job `failed` ⇒ no text, forever, silently.** Plus a narrower case, seen document + no ocr
  job at all, which the current code should make impossible and which §5.4 turns into a checked
  invariant rather than an assumption.

## 4. Design principles for the rest of this document

Three constraints did the shaping, and each one is measured rather than stylistic:

1. **Discovery cannot scan Papra for this.** The listing payload carries no content field of
   any kind (`probe_e_discovery_shape.py`: listing document keys are
   `createdAt, createdBy, customProperties, deletedAt, deletedBy, documentDate, id, isDeleted,
   mimeType, name, notes, organizationId, originalName, originalSha256Hash, originalSize, tags,
   updatedAt` — no `content`, no `contentLength`, no extraction flag). Only the document detail
   carries `content`, inline. So "which of my 10,000 documents has no text?" costs 10,000 detail
   requests, each returning the whole extracted text. That rules out a periodic org-wide audit
   and pushes discovery onto local state.
2. **Emptiness is unambiguous once you look.** `PATCH(content="")` returns 200 and reads back as
   `''` (type `str`), not `null`. So `not content` is a sound predicate for "this document has no
   text", and it can be checked per candidate without parsing anything.
3. **No schema change.** `jobs` already has `status`, `attempts`, `error`, `payload` (JSON) and
   `updated_at`; `settings` is a key/value table. Everything the design needs fits in them, so
   this spec adds no migration — which also means it cannot collide with the migration that
   `ingest-visibility` is adding.

## 5. Design

### 5.1 Deployment: the flag, and proof that it took

- `docker-compose.test.yml` and the operator's production compose set
  `DOCUMENTS_CONTENT_EXTRACTION_ENABLED: "false"` on the Papra service.
  Setting it in the **test** compose is deliberate: integration tests must run in the same
  topology production runs in, or the tests certify a configuration nobody deploys.
- A new integration test asserts the property rather than the environment variable: upload a
  document, do not `PATCH`, poll for 15 s, assert `content` stayed empty. This is the only form
  of the check that survives a renamed variable, a typo, or an upstream default flip — all three
  of which leave the variable silently unset and the race silently back.
- **Content-present detection, because an env-var-only fix has no compile-time existence.** In the
  recovery pass (§5.2), a candidate whose detail shows **non-empty** content while its ocr job is
  `failed` is skipped and the pass logs
  `WARNING ocr job failed but content is present for <document>; skipping automatic recovery —
  this does not establish who wrote the content`.
  The causes cannot be told apart from here: the `PATCH` may have succeeded while its response was
  lost and a later attempt failed; someone may have edited the document in Papra's UI; another ocr
  job for the same document may have succeeded; the text may predate this deployment; or the flag
  may have moved. Only the last of those is a configuration fault, so the line reports the
  observable and names no culprit. The configuration question is answered where it can be answered
  — by the integration test above, which asserts the behaviour instead of the variable.

`check` gains one line from counts gathered while the store is open: `ocr jobs: pending N,
failed N`. It counts **jobs**, and it has to stay worded that way: with extraction off a failed ocr
job usually means a document with no text, but the case just described is the counter-example, so a
line asserting "N documents have no text" would be wrong in the presence of the very state the
recovery pass is reporting.

### 5.2 Recovery: find the candidates locally, act only on verified emptiness

A new pass, `recover_missing_content(store, papra, *, limit)`, run from the sweep loop's
heartbeat — the same place `ingest-visibility` put the ingest-events prune (the pass that trims
rows older than the 90-day window) — because it is a slow background repair and inherits that
loop's already-decided interval, pagination, and error isolation. It is called outside the
sweep's `try`/`except` for the same reason the prune is: a failing repair must not swallow the
sweep's summary, which is the one line telling the operator the sweep is still working.

Candidate discovery, local SQL only:

- ocr jobs with `status = 'failed'`;
- plus, for completeness against a restored or hand-edited database, documents in `documents`
  that have no ocr job row for their `(papra_org_id, papra_document_id)`. That set should always be
  empty (§5.4), and its repair is not a reset — there is nothing to reset — so the pass downloads
  the file, applies intake's own size and format gates, computes the file hash, and goes through
  `enqueue_ocr_job`, which is the enqueue-before-seen path intake uses. It is the expensive
  candidate class (a download each), so it shares the same `limit`.

**Fair traversal, because a bounded pass over failed jobs starves.** The obvious query — failed
jobs, `ORDER BY created_at`, `LIMIT n` — rereads the same rows every pass: the deleted ones, the
ones whose content turned out to be present, the ones at the ceiling. They stay `failed`
permanently, they fill every page, and a repairable document queued behind them is never reached.
So the pass walks candidates by `jobs.rowid` and remembers the last rowid it examined in `settings`
under `recovery_cursor`, wrapping to the start when the query comes back empty. That survives a
restart for free and reaches every candidate however hostile the skips are.

Per candidate examined in a pass:

1. `GET` the document detail. Skip and log if it 404s or reports `isDeleted` — a document Papra no
   longer has can never gain text, and re-OCRing it would spin forever. (Both signals exist:
   `isDeleted`/`deletedAt` are in the listing and detail payloads, measured in probe E.)
2. If `content` is non-empty: skip, and log the content-present line of §5.1.
3. If another ocr job for the same `(org, document_id)` is `pending` or `running`: skip. The
   idempotency key includes the content hash, so **the unique index does not give one ocr job per
   document** — a re-ingest at a different hash, or the candidate class above, can leave two, and
   reviving an old failed one while a fresh one is in flight is a self-inflicted race.
4. Otherwise revive it and count the round in a single conditional update:
   `UPDATE jobs SET status='pending', attempts=0, error=NULL, next_attempt_at=?, payload=<merged>`
   `WHERE id=? AND status='failed'`. The guard is what stops two passes, or a pass and the
   `jobs --reset` command, from both claiming one repair; the payload is merged rather than
   replaced so whatever intake stored in it survives. The counter is `"recovery_rounds": n` inside
   the existing JSON `payload` — no column, no migration.

Ceiling: at `recovery_rounds == 3` the pass stops reviving and logs once at `ERROR` with the
document id, the last stored `error`, and the §5.3 command that overrides it; the payload records
that the line was emitted, so it appears on the transition rather than on every heartbeat after.
Three rounds means up to three additional *job runs*, each of which may burn the worker's three
attempts with its backoff — minutes of work for one document in the worst case, which is what not
silently giving up costs. Without a ceiling, a document failing for a reason OCR cannot fix (a bad
model name surfaces as a 4xx on the LLM call) is revived forever.

**What the verification buys, and what it does not.** Reading the content before reviving is an
eligibility check, not exclusion: between that read and the worker's `PATCH`, an edit in Papra's UI
or a competing job can put text there. So the worker re-reads the document immediately before
writing and skips the `PATCH` — logging that it skipped — if content became non-empty meanwhile.
Papra exposes no compare-and-set on `content`, so best-effort is the strongest claim available, and
this spec will not imply atomic exclusion of a human with a text box.

Reviving by reset rather than re-insert is forced by the schema:
`CREATE UNIQUE INDEX idx_jobs_idempotency ON jobs(kind, papra_org_id, document_id, content_hash)`,
and `enqueue_job` is `INSERT OR IGNORE`. An enqueue for the same document and file hash is a silent
no-op — which is why `reset_job`'s shape (`status='pending', attempts=0, error=NULL`) is the right
primitive, extended with the `AND status='failed'` guard and the payload merge above.

**If the sweep is switched off, recovery disappears with it.** The pass hangs off the sweep loop, so
`sweep_interval_s = 0` means failed jobs are never recovered and nothing says so. Startup must then
log `WARNING sweep disabled: failed ocr jobs will not be recovered automatically`, rather than
letting the operator discover it from a backlog that looks empty because nothing is being tried.

Deliberately **not** written to `ingest_events`: that table's `kind` is a closed set of *mail
attachment dispositions*, validated in `record_ingest_event`, and a background repair on an
already-ingested document is not one of them. Stretching it would mean editing a closed set to
describe something the table was not about. Recovery is reported where it is discovered — the
log, and the `check` counts of §5.1.

The `title` job has the identical shape of hole (failed title job ⇒ the document keeps its
upload filename). Out of scope here, deliberately: a wrong name is cosmetic next to no text, the
repair is the same primitive, and §5.3's CLI makes it operator-repairable the moment this lands.
Say so rather than let it look overlooked.

### 5.3 Operator tooling: make the manual path exist

`reset_job` with no caller is how this class of bug stops being an outage on the day the
automatic policy says "no more". Add a `jobs` subcommand, read-mostly and following the
`events` command's established rules (never migrate, refuse a missing database **before**
constructing `Store()`, which would otherwise create the file):

- `papra-suite jobs --status failed --kind ocr [--limit N] [--json]` — list candidates with
  document id, attempts, and the stored `error`.
- `papra-suite jobs --reset JOB_ID` — one job back to `pending`, clearing the recovery counter in
  `payload`. This is the escape hatch §5.2's ceiling hands to the operator.
- `papra-suite verify-content [--org-id ID] [--requeue]` — the read-back audit that discovery
  cannot automate (§4, principle 1): walk the org's documents page by page, fetch each detail, and
  classify every document whose `content` is empty into exactly one of four buckets, because they
  call for opposite responses:
  **(a)** an ocr job `failed` → reset it;
  **(b)** no ocr job at all → enqueue through the download-and-hash path (§5.2, class 2);
  **(c)** an ocr job `done` and content still empty → *nothing was lost*: our OCR returned nothing,
  which is the correct outcome for a blank page, and re-OCRing it would burn a model call on a page
  with no text. Reported, never requeued, unless `--requeue-done` is typed explicitly — a document
  whose text vanished *after* a successful write lands in this bucket too, which is the honest cost
  of a heuristic that has to guess;
  **(d)** `pending` or `running` → reported as in flight, never touched, because resetting a job out
  from under a worker corrupts that worker's transition bookkeeping (§5.2's ordering note).
  Documented as the after-a-Papra-restore tool rather than a scheduled pass: its cost is one detail
  request per document plus the full extracted text of every one. It is also the only thing that can
  see a `done` job whose content disappeared later — local failed-job discovery cannot, and §5.2
  deliberately does not look for it.

All three render through the same untrusted-text escaping discipline already used for `events`
(document names and stored error strings are attacker-influenced: the name comes from a mail
attachment, and `PapraError.__str__` embeds 200 characters of Papra's response body).

### 5.4 Invariants

1. With the flag set, exactly one process writes `content` — as a matter of configuration. Recovery
   narrows the human-shaped exceptions (§5.2's last paragraph) but cannot exclude them, so this is a
   deployment property, not something the code enforces.
2. Every document in `documents` has at least one ocr job row for its `(org, document_id)`. The
   recovery pass heals violations through §5.2's class-2 route and logs them; the set is expected to
   stay empty and the log line is the signal that it has not.
3. A document with empty `content` is in exactly one of four states: queued or running; inside the
   recovery ceiling; named by the ceiling `ERROR`; or legitimately blank because our OCR produced
   nothing (§5.3 bucket c). That last one is why "empty" and "missing" are never the same predicate
   anywhere in this design.
4. A job is revived only when its document was verified empty, only while it is still `failed`, and
   only when no other ocr job for that document is in flight.
5. Every candidate is examined eventually: the pass advances a persisted cursor instead of rereading
   a fixed head of the backlog, examines at most `limit` candidates per pass, and cannot revive the
   same job twice in one round.
6. No migration, no new table, no new column.

## 6. Test plan

The helper-level tests prove the policy; they cannot prove the product runs it, because a pass that
is never invoked passes every one of them. The runtime tests at the end are therefore the release
gates.

Unit (fake Papra transport, existing fixtures):

- failed ocr job + empty content ⇒ revived to `pending`, `recovery_rounds` 1, and whatever intake
  had stored in `payload` still present.
- failed ocr job + non-empty content ⇒ not revived, content-present line emitted, and that line
  names no writer.
- document 404 / `isDeleted` ⇒ skipped, not revived, no spin.
- another ocr job for the same document `pending` or `running` ⇒ not revived.
- job that left `failed` between selection and the conditional update ⇒ zero rows updated, counter
  unchanged.
- ceiling ⇒ `ERROR` on the transition only, fourth round silent, counter still there after the store
  is closed and reopened.
- seen document with no ocr job ⇒ file downloaded, intake's size and format gates applied, hash
  computed, job enqueued through `enqueue_ocr_job`.
- starvation ⇒ with more non-actionable candidates (deleted, content-present, at ceiling) than
  `limit` standing *before* a repairable document, that document is reached within a bounded number
  of passes, and the cursor holds across a simulated restart.
- `limit` respected; one candidate raising `PapraError` does not abort the pass.
- the pass raising wholesale does not swallow the sweep pass's summary (mirrors the existing
  prune-side test), and `sweep_interval_s = 0` emits the "recovery will not run" startup line.
- worker skips the `PATCH` when content became non-empty after the job was claimed.
- `jobs --reset` clears the counter; `verify-content` classifies buckets (a)–(d) correctly,
  `--requeue` touches only (a) and (b), bucket (c) needs `--requeue-done`.
- renderer escaping for document names and error strings (reuse the `events` test's control-char,
  C1, and U+2028/U+2029 cases).

Integration, against the pinned stack, which now carries the flag:

- upload without `PATCH` ⇒ `content` empty after 15 s (§5.1's proof).
- OCR a scan, `PATCH` at +0 s, wait 60 s ⇒ our text still byte-exact, and content search for a token
  unique to it hits. This is the regression test for the original bug: it fails on the default
  configuration and passes on the pinned one.
- `Straße`-containing content written by the stubbed OCR ⇒ findable, pinning the German regression
  and not just the survival.
- **end-to-end through the runtime**: a stubbed ocr that fails until its third attempt leaves the job
  `failed`; `serve` then runs with the stub fixed, and without any test calling
  `recover_missing_content` the document ends up with byte-exact text and a search hit on a
  PATCH-only token. Deleting the sweep-loop invocation must fail this test.
- the same shape driven through the sweep heartbeat with more non-actionable candidates than `limit`,
  proving eventual recovery across passes *and* across a restart of `serve`.
- injected crash-before-enqueue, lost `PATCH` response, worker death, exhausted retries ⇒ each ends
  with content present or one specific `ERROR`, discovered by the runtime rather than by a
  hand-called helper.
- a blank page (OCR result `""`) ⇒ `PATCH`ed empty, job `done`, and neither the recovery pass nor
  `verify-content --requeue` ever re-OCRs it.

Gate: `make lint`, `make test-unit`, `PAPRA_REQUIRE_STACK=1 make test-integration`, plus mutation
checks — delete the sweep-loop invocation, delete the `status='failed'` guard, delete the in-flight
check, delete the `PATCH`-time re-read. Each must fail a named test.

## 7. Measured facts this spec leans on

| fact | source |
| --- | --- |
| Papra's extraction overwrites our text when it lands second (t+2 s, permanently) | `probe_c2_exact.py` |
| With the flag off, our text is byte-exact for 90 s and Papra never writes | `probe_c2_exact.py` |
| Default extraction is OCR (`eng.traineddata`), accurate on clean print | `probe_a_defaults.py` |
| German degrades: `Straße`/`Grüße`/`Köln` all unfindable; mangle varies per run | `probe_a2_harder_inputs.py` |
| `custom-http`: async (upload 0.1 s), multipart `file`, no credential sent, text stored verbatim | `probe_b_custom_http.py` |
| one request on 500 and on timeout over 180 s; late answer discarded; content stays empty | `probe_b2_retry.py` |
| listing has no content field; detail carries `content` inline; `PATCH(content='')` reads back `''`; `isDeleted`/`deletedAt` present | `probe_e_discovery_shape.py` |
| `UNIQUE (kind, org, document_id, content_hash)` + `INSERT OR IGNORE` ⇒ reset, not insert | `store.py:74-80`, `enqueue_job` |
| sweep skips seen documents; permanent-failure triggers; `reset_job` has no caller | `pipeline.py`, `store.py` |

## 8. Not established

- What else the flag changes inside Papra. Only `content` and content-search were verified; the
  UI's document view, Papra's intake email, and any status affordance were not examined.
- Whether `deu.traineddata` plus `DOCUMENTS_OCR_LANGUAGES` makes Papra's own text good enough for
  German. Probably not needed (we already have a local VLM and the local cost is free), but if it
  did, it would still not remove the race.
- Whether Papra re-extracts existing documents after a restart or an image upgrade. If it does,
  re-enabling extraction later (rollback) would rewrite text on documents we already own, not just
  new ones. Unmeasured.
- Anything about extraction behaviour beyond the 180 s window, and any real photograph: no
  rotated/skewed fixture exists, because building one would add a dependency to a probe.
- Whether recovery's `limit`/ceiling numbers are right for the operator's actual corpus size. They
  are conservative defaults, not tuned values.

## 9. Operator decision: the corpus that already has text

This change fixes documents from the moment it deploys. It does **not** touch what is already in
Papra, because recovery writes only where content is empty — and the existing corpus is not empty.
Some of it is Papra's English-model OCR: umlauts stripped, `ß` mangled, those documents unfindable
by exactly the words on the page. **How much** is not known. The evidence measured fixtures, not the
corpus, and any document whose `PATCH` settled after extraction already holds our text.

So the decision should be preceded by a count rather than by a judgement. `verify-content`'s walk
can, in the same pass, report a mangle signature — umlauts systematically absent where the page
printed them, `ß` replaced by `B`-shaped noise — and the number it prints is the size of the
problem. Adding that classifier to the loop it already walks is cheap; it turns "re-OCR
everything?" into arithmetic instead of taste.

Then the two options:

1. **Re-OCR what the survey flags**, not the whole corpus by default. Cheap in money, expensive in
   wall-clock and a burst of load on the local VLM. Mechanically `verify-content --requeue-mangled`;
   it is the one place this design overwrites non-empty content, so it is a flag a human typed,
   never a scheduled behaviour.
2. **Accept mixed quality**: new documents correct, old ones as they are, letting them age out.

Recommendation: run the survey first, then take option 1 if it reports a meaningful count, because
the German failure is silent — searching `Straße` returns nothing and gives no hint that a document
exists. Do not launch a full-corpus re-OCR on the strength of fixture evidence.

## 10. Explicit non-goals

- Making Papra's own extraction better (see §8, second bullet).
- Coverage for documents that arrive in Papra by another route, **beyond what the sweep already
  provides**. `sweep_once` is explicitly "record and enqueue OCR for externally-ingested
  documents", so a PDF uploaded through Papra's UI or its intake email *is* picked up, OCR'd, and
  written — in every configured org, on the sweep's interval. What that does not cover: non-PDF
  formats, which `_is_pdf` skips, and the interval itself. With extraction off, such a document has
  no text between arriving and the sweep reaching it, where previously it got Papra's own text
  within seconds. If that gap ever becomes unacceptable, §2's `custom-http` measurement is the
  starting point, together with a bounded-window sweeper for its silent-failure mode.
- Title/name recovery (§5.2's last paragraph) and tag-based surfacing of failures. Note that the
  product does not use Papra's tags at all today (`add_tag`/`list_tags` exist on the client and
  have no callers), so tags would be new surface, not an extension of a pattern.
- The MCP search server. It reads `content`; it does not care who wrote it, and it must not gain
  any repair behaviour. Its plan stays approved-and-unimplemented and independent of this spec.

## 11. Sequencing

1. Merge `ingest-visibility`, then `ocr-prompt-language`, into `simplify-ingest`. This spec adds no
   migration, so the order costs nothing; the `check` line and the CLI conventions land on top of
   that work, and rebasing afterwards would be churn.
2. **Land and deploy the recovery path first** — pass, `check` counts, `jobs` CLI — with extraction
   still enabled. On that configuration recovery candidates are rare and a miss is survivable,
   which is the right environment in which to find out whether the traversal and the ceiling
   actually work. Flipping the flag first would mean discovering recovery bugs at the exact moment
   recovery becomes the only thing standing between a failed job and a permanently text-less
   document.
3. Then flip the flag in both compose files, with the "no `PATCH` ⇒ stays empty" integration test.
   That test fails until the flag is honoured, which is the point.
4. `verify-content` and the §9 survey, then the operator's decision.

Rollback is the flag alone, and it is not free: setting extraction back to enabled puts the racing
writer back for every subsequent upload, and §8 leaves open whether it also rewrites documents that
are already ours.
