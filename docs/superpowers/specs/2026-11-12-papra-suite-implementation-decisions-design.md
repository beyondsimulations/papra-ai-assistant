# Papra Suite — implementation decisions

Companion to `spec.md` (the build-to spec). This document records the
implementation-level decisions made during the design brainstorm on
2026-11-12, including the places where they sharpen or deviate from
`spec.md`. Where the two disagree, this document wins; each deviation is
flagged inline.

## 1. OCR concurrency

- Up to **3 OCR jobs (documents) in flight** at once, enforced by an
  asyncio semaphore. Env var `OCR_CONCURRENCY`, default 3, hard-capped at 3.
- Within one document, pages are processed **strictly sequentially** —
  one vision request in flight per document. Page-level pipelining is
  out of scope for v1.
- Rationale: the MLX server is VRAM-bound; 3 concurrent documents give
  burst throughput without overloading it. The DB-backed queue absorbs
  anything beyond the semaphore.

## 2. Job chaining: central transition handler

- All job completions flow through **one transition function** in
  `pipeline.py`: `on_job_done(job) -> next job spec | None`. The chain is
  `ocr → title` (see §3 for why there is no upload job kind).
- Workers never enqueue successors; they mark their job done and call
  the transition handler. The chain is readable in a single function,
  and dashboard retry buttons re-enter the chain through the same code
  path.
- When the OCR job completes, the handler upserts the title job keyed on
  the **new OCR content hash**. Title jobs from older content hashes
  remain in the table as history; consumers always take the latest.

## 3. Mail poller: synchronous upload, no upload job kind

Deviation from `spec.md` §4 ("one worker loop per job kind" — the kinds
are now `ocr` and `title` only) and a sharpening of §5.3.

- The poller uploads each PDF to Papra **synchronously**, inline in the
  poll task, using the shared retry policy from the Papra client.
  Rationale: uploads are fast internal-network calls; doing them inline
  makes mark-as-read and delete-after-download trivial (they happen
  right there, after all of the message's PDFs uploaded), and the jobs
  table carries only the slow, retry-worthy kinds.
- The crash window between "upload succeeded" and "dedup hash
  recorded" is milliseconds and accepted under the spec's at-least-once
  stance (§4).
- Async structure: one asyncio poll task per mailbox
  (`poll → sleep(poll_interval)`), a per-mailbox lock against overlap,
  and all blocking `imaplib` calls executed via `asyncio.to_thread`
  through a single sync `MailboxSession` class. The async/sync boundary
  lives only in `mail.py`.
- UID tracking: `last_seen_uid` + `uidvalidity` stored per mailbox; a
  UIDVALIDITY change resets `last_seen_uid`.

## 4. Document sweep: OCR for externally-ingested documents (in v1 scope)

Closes the gap in `spec.md` §1 ("for each new document"): documents
entering Papra by other routes (manual upload, Papra intake emails)
also get OCR and titles.

- A background sweep task per organization polls
  `GET /api/organizations/:orgId/documents?sortField=createdAt&sortOrder=desc`
  every `SWEEP_INTERVAL_S` (default 300).
- New table `documents` (papra_org_id, papra_document_id, name, mime,
  ingested_via, first_seen_at): every document the service has enqueued
  OCR for is recorded here, regardless of ingestion path.
- For each listed document not yet in the table: fetch metadata, record
  it, and if it is a PDF, enqueue an OCR job (idempotent — see §6).
  Non-PDFs are recorded and skipped so they are never re-checked.
  Documents uploaded by the mail poller are already in the table; the
  sweep's enqueue for them is a silent no-op.
- Env var additions: `SWEEP_INTERVAL_S` (default 300, `0` disables the
  sweep).

## 5. Dashboard: Jinja2, zero client-side JS

- **Jinja2 is added** to the pinned dependency list (deviation from
  `spec.md` §12's constraint list; hand-rolled templates for four
  pages are false economy).
- Server-rendered pages, plain HTML forms that POST and 303-redirect,
  no client-side JavaScript at all — satisfying acceptance criterion 6
  by construction. Retry/regenerate buttons are `<form>` submit
  buttons.

## 6. Retry semantics: one row per operation, rewind — don't duplicate

- One job row per logical operation. Unique index on
  `(kind, papra_org_id, document_id, content_hash)` across **all**
  statuses; enqueue is `INSERT ... ON CONFLICT DO NOTHING`. The hash is
  the **source file hash** for OCR jobs and the **OCR output hash** for
  title jobs, so a re-run OCR with changed content yields a new title
  job while an unchanged re-run does not. Restart,
  double-click, sweep, or a future webhook can never create a second
  active job for the same work.
- **Retry button** = `UPDATE jobs SET status='pending', attempts=0,
  error=NULL`. **Regenerate title** = the same reset applied to a
  `done` title job (a fresh LLM roll). **Retry OCR** resets only the
  OCR row; the cascade to a new title job happens automatically via
  the transition handler (§2).
- Auto-retry: `attempts < 3`, exponential backoff via a
  `next_attempt_at` column; the worker loop claims
  `status='pending' AND next_attempt_at <= now`. After 3 attempts →
  `failed`; only the dashboard button revives it. Papra 4xx (except
  429) never auto-retries and fails permanently per `spec.md` §5.2.
- Mail upload dedup (`mailbox, message-id, filename, size` hashes) and
  job idempotency are **separate tables**: retrying a job can never
  trip upload dedup, and re-polling a message can never duplicate a
  document.

## 7. Webhooks: out of scope for v1

Verified upstream: Papra ships a webhooks module
(`document:created|updated|deleted`, tag events, Standard-Webhooks
signed) with SSRF protection and a `WEBHOOK_URL_ALLOWED_HOSTNAMES`
escape hatch. A webhook receiver would add a listening endpoint and
operator config to learn things the service already knows (its own
uploads) — and the sweep (§4) covers externally-ingested documents
without either. Deferred to v2; the sweep's `documents` table and
idempotent enqueue make a webhook receiver a drop-in addition later.

## 8. Test orchestration: docker compose + in-process mock LLM

- `docker-compose.test.yml` provides the third-party services:
  **GreenMail** (IMAP) and a **scratch Papra** container.
- The mock OpenAI-compatible server is our own fake: an in-process
  pytest fixture serving on a random port. No container needed.
- One command (`make test`): compose up, wait for health, pytest,
  compose down.
- Known wrinkle for milestone 1: the scratch Papra needs a user, an
  org, and an API key seeded programmatically before tests can talk
  to it.

## 9. Env var summary (additions to `spec.md` §6)

| Var | Default | Meaning |
|---|---|---|
| `OCR_CONCURRENCY` | 3 | Max concurrent OCR jobs (hard cap 3) |
| `SWEEP_INTERVAL_S` | 300 | Sweep interval; `0` disables the sweep |

## 10. Milestone impact

- Milestone 3 gains the sweep task (it enqueues OCR jobs, so it lands
  with the OCR worker) and the `documents` table.
- Milestone 1 records the real API surface: bearer auth, upload
  `POST /api/organizations/:orgId/documents` (form-data `file`), write-back
  `PATCH .../documents/:documentId` (`name`, `content` — one endpoint
  serves both OCR content and titles), list+search
  `GET .../documents?searchQuery=`, file download
  `GET .../documents/:documentId/file`, tags list/add. Verified against
  docs.papra.app on 2026-11-12; re-verify against the running
  instance's OpenAPI document at build time per `spec.md` §5.2.
