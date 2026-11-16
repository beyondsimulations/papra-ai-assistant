# Papra Suite: single-service spec

One Python service that replaces papra-companion + papra-ai + papra-mcp: email ingestion, vision-model OCR, AI title generation, an MCP server, and a single-password dashboard. No users, no multi-tenant auth, no public exposure. Build-to spec for coding agents.

## 1. Goals

- Poll IMAP mailboxes on a schedule, download PDF attachments, and upload them as documents to an existing Papra instance per organization. PDFs only: every other attachment type is out of scope.
- For each new document: run vision-model OCR on PDFs and write the extracted markdown back to the document content in Papra, so full-text search works.
- Generate a human-readable title from the document (content if available, else filename) and write it back as the document name.
- Expose the same functionality over MCP (stdio) for an MCP client, talking to Papra via its REST API.
- Provide a password-protected dashboard (one shared password, session cookie, no user accounts) to configure mailboxes, models, view the queue, trigger reprocessing, and read logs.
- Run as one container on an existing Docker network next to Papra, reachable only over localhost or the tailnet.

## 2. Non-goals

- No user management, roles, or registration. One password in an env var.
- No non-PDF ingestion. `application/pdf` is the only accepted attachment mime type. Everything else (images, office documents, zip, calendars) is ignored and logged. The OCR worker therefore only ever sees PDFs.
- No sending email.
- No public internet exposure of anything, including the dashboard.
- No replacing Papra itself. Papra stays the document store and web UI; this service never stores documents, only metadata and credentials in its SQLite file.
- No support for LLM providers other than OpenAI-compatible endpoints. The target endpoint is a local MLX server.
- No folder-ingestion transport in v1; the service uploads via the Papra API directly (rationale: the upload response returns the document id, which removes all polling and the ingestion-folder permission issues entirely).

## 3. Target environment (generic; fill values at deploy time)

- Papra: self-hosted, reached internally by container name (e.g. `http://papra:1221`) on a shared Docker network; externally behind whatever reverse proxy or VPN the operator uses. Runs as uid 1000.
- API auth: API key (`ppapi_...`) created in Papra under Settings → API keys. Verify the exact auth header against the OpenAPI document of the running instance (see §9).
- Multiple organizations, ids `org_...`, visible in the Papra URL.
- LLM endpoint: any OpenAI-compatible server (vLLM, Ollama, LM Studio, MLX-based); discovered via `GET /v1/models`. Needs one text model for titles and one vision model for OCR (a Qwen3-VL or Qwen2.5-VL class model is the reference choice for document work).
- Deployment host: Docker with a compose tool (plain docker-compose or Dokploy). The operator needs one external network name for the compose file and one host for SSH access.
- Document languages: configurable per prompt; reference deployment processes German and English.

## 4. Architecture

Single asyncio process. Five modules plus a shared store:

```
papra_suite/
  __main__.py          # CLI: serve | mcp
  config.py            # env parsing, validation at startup
  store.py             # SQLite (WAL) at /app/data/state.db
  papra_client.py      # thin async REST client for Papra
  llm_client.py        # thin async OpenAI-compatible chat client
  mail.py              # IMAP poller task
  ocr.py               # PDF → page images → vision model → markdown
  titles.py            # content/filename → title
  pipeline.py          # orchestration + job queue (DB-backed)
  dashboard/           # FastAPI app: login, pages, JSON API
  mcp.py               # FastMCP server (stdio), separate entrypoint
```

- Task model: DB-backed jobs table, one worker loop per job kind, at-least-once processing with idempotency keys (document id + job kind + content hash). Survives restarts; no in-memory-only state.
- The dashboard and the workers live in one process; the MCP server is a separate process (`python -m papra_suite mcp`) so an MCP client can spawn it over stdio.
- All outbound calls: IMAP (mail provider), Papra API (internal Docker network), LLM endpoint (tailnet). Nothing listens except the dashboard port.

## 5. Component specs

### 5.1 Store (SQLite)

Tables: `orgs` (id, papra_org_id, name), `mailboxes` (id, org_id, host, port, username, password, ssl_mode, poll_interval_s, subject_filter, delete_after_download), `jobs` (id, kind, papra_org_id, document_id, payload JSON, status, attempts, error, created_at, updated_at), `settings` (key/value). Store IMAP credentials in the DB (single-user service on a private host; acceptable risk, noted in dashboard UI). No migration framework needed; use `PRAGMA user_version` with a tiny migration list.

### 5.2 Papra client

- Wrap the endpoints actually needed; do not guess them. At build time, read the OpenAPI document of the running instance (`/api-reference` or the docs at docs.papra.app/api-reference) and use exactly those paths/params. Needed operations:
  - upload document (multipart) to an organization
  - get document metadata + content
  - update document (name, content)
  - list documents (with search/filter, for dashboard and MCP search tool)
  - download the original file
  - list tags / add tag to document (for the dashboard and future tagging hooks)
- One retry policy: 3 attempts, exponential backoff, all 5xx and network errors; 4xx (except 429) marked failed permanently with the error message stored.

### 5.3 Mail poller

- Per mailbox: connect (IMAP with SSL or STARTTLS per config), fetch unread messages since last seen UID, process only attachments with mime type `application/pdf` (sniff by magic bytes `%PDF-`, not only the declared mime type: mail clients frequently mislabel). Non-PDF attachments are logged and skipped; a message containing only non-PDF attachments is not counted as processed.
- Admission is by SENDER, not by subject (changed 2026-11-15; the per-mailbox
  subject regex was removed — see
  `docs/superpowers/specs/2026-11-15-ingest-simplification-and-sender-allowlist-design.md`).
  The deployment is one dedicated inbox that only the operator forwards into: a
  human decides what belongs in Papra, so nothing reads the subject to decide.
  Admission is decided on the fetched HEADERS, before a body is requested.
  An allow-list entry is an exact address or a whole domain; an empty or absent
  `PAPRA_ALLOWED_SENDERS` PAUSES ingestion rather than retiring mail; a
  malformed `From:` is refused as ambiguous rather than interpreted. It filters
  mail CLAIMING an admitted identity — it does not defeat spoofing.
- Dedup: hash (mailbox, message-id, attachment filename, attachment size); skip known hashes.
- Filename handling: sanitize; keep original name as the Papra document name initially (title generation replaces it later).
- Optional delete-after-download per mailbox, default false, only mark as read otherwise. If enabled: a message is deleted only after every PDF attachment in it was uploaded successfully; a message with failed uploads stays in the mailbox for the next poll (dedup prevents double uploads).
- Errors never crash the loop: log, continue, back off on repeated auth failures (stop the mailbox after 3 consecutive failures, show red in dashboard until an operator retries).

### 5.4 OCR worker (PDFs only)

- Input: document id. Download the file from Papra, render each page to PNG at ~150 DPI with PyMuPDF, base64 the page, send one chat-completions request per page with the page image as an image content part and a fixed system prompt: "Transcribe this document page to clean markdown. Preserve structure: headings, lists, tables. Output only the markdown. Text is German or English."
- Guard rails: skip pages over a max pixel budget (configurable, default ~2 MP per page) by downscaling; hard cap pages per document (default 50) with a dashboard warning; time out single requests (default 300 s, the MLX server can be slow on cold models); on model overload/timeout, retry with backoff, max 3 attempts, then mark failed and show in dashboard.
- Assemble page outputs into one markdown document, write back to Papra via the update-document endpoint.

### 5.5 Title worker

- Input: document id, after the OCR job for the same document completes (OCR always runs in this design, since every ingested document is a PDF).
- Prompt: OCR content excerpt (first ~2000 chars) plus original filename; output: one concise German-or-English title, max ~80 chars, no quotes. Fallback to filename if the LLM fails.
- Write back as document name.

### 5.6 MCP server

- `python -m papra_suite mcp`, stdio, FastMCP. Tools: `search_documents(query)`, `get_document(document_id)` (metadata + content), `list_recent_documents(limit)`, `download_document_path(document_id)` (downloads to a temp dir, returns the path). Env: `PAPRA_BASE_URL` (the operator's external Papra URL), `PAPRA_API_KEY`. No tool may modify or delete anything in v1.

### 5.7 Dashboard

- FastAPI, served on one port (default 8080). Bind configurable; for this deployment `127.0.0.1:8080` or the tailnet IP, never `0.0.0.0` unless explicitly set.
- Auth: `ADMIN_PASSWORD` env var; POST login sets a signed session cookie (itsdangerous-style, no dependency on a framework session plugin); every page and JSON route behind it. No rate limiting needed on a tailnet-only port, but enforce a constant-time compare and a 1 s sleep on failed login.
- Pages (server-rendered, minimal CSS, no JS framework):
  1. Status: queue depth, running/failed jobs, per-mailbox last poll and error state, LLM endpoint health (ping `GET /v1/models`), Papra reachability.
  2. Mailboxes: CRUD per org, test-connection button (IMAP login + one LIST), subject filter field.
  3. Documents: recent 50 with title, org, OCR status; per-document buttons: retry OCR, regenerate title, view last error.
  4. Logs: last 500 log lines, plain text.

## 6. Configuration

Env vars (all required unless marked optional): `PAPRA_BASE_URL` (internal, e.g. `http://papra:1221`), `PAPRA_API_KEY`, `LLM_BASE_URL`, `LLM_API_KEY`, `TITLE_MODEL`, `OCR_MODEL`, `ADMIN_PASSWORD`, `DASHBOARD_BIND` (default `127.0.0.1:8080`), `DATA_DIR` (default `/app/data`), `OCR_MAX_PAGES` (default 50), `OCR_PAGE_PIXEL_BUDGET` (default 2000000), `OCR_DPI` (default 150), `PAPRA_ALLOWED_SENDERS` (comma-separated addresses and
domains; **required in practice**: with no usable entry the poller pauses instead
of ingesting). Mailbox config lives in the dashboard/DB, not env.

## 7. Deployment

- One Dockerfile: python:3.12-slim, non-root user uid 1000, `pip install` from a pinned `requirements.txt`, healthcheck on dashboard login page, VOLUME `/app/data`.
- One compose service, joins an external network whose name comes from `${PAPRA_NETWORK}` in the `.env`, one named volume for `/app/data`. Note for the deployer: chown the volume to 1000:1000 once after first start (Docker creates it root-owned; the app runs as 1000 and will crash otherwise).
- Compose-tool notes (applies to Dokploy and similar): env vars via the tool's environment editor, redeploy after changes. Do not set `container_name`. Do not bind 80/443.
- No ports published to the LAN. Dashboard access via SSH tunnel (`ssh -L 8080:127.0.0.1:8080 <host>`) or bind to a VPN interface address (e.g. a tailnet IP) for direct private-network access.

## 8. Acceptance criteria

1. Drop a mail with a German PDF attachment into a configured mailbox → within one poll interval the document exists in the correct org, has a generated title, and its content is searchable in Papra within the OCR time budget.
2. A mail containing only non-PDF attachments produces no document, one log line, and no error state.
3. Restart the service mid-queue → jobs resume; no duplicate documents created (dedup hash holds).
4. LLM endpoint down → jobs fail with visible errors in the dashboard; endpoint back up → retry button clears them.
5. MCP: `search_documents` finds a document by a phrase that only occurs in OCR-written content, not the filename.
6. Dashboard: wrong password rejected; no page reachable without the cookie; all pages functional with JS disabled.
7. No port of the service reachable from the LAN interface (verify with `ss -tlnp`).
8. `docker compose config` passes; volume permission fix documented in README.

## 9. Links and references

- Papra: [github.com/papra-hq/papra](https://github.com/papra-hq/papra) · docs: [docs.papra.app](https://docs.papra.app/) · API: [api endpoints](https://docs.papra.app/resources/api-endpoints/) · [content extraction](https://docs.papra.app/guides/content-extraction/)
- [papra-companion](https://github.com/remotesojourner/papra-companion) — prior art for the mail half (C#; being replaced by this service)
- [papra-ai](https://github.com/koljam/papra-ai) — prior art for the OCR half (TypeScript; being replaced by this service)
- [papra-mcp](https://github.com/Rigare/papra-mcp) — prior art for the MCP half (Python; being replaced by this service)

## 10. License constraints (verified per repo, 2026-11)

| Repo | License | Rule for agents |
|---|---|---|
| papra-hq/papra | AGPL-3.0 | Read for facts only (API paths, shapes, behavior). Never copy code into this service: substantial copying could make it an AGPL derivative. |
| Rigare/papra-mcp | Apache-2.0 | Copying permitted with notice preservation, but prefer clean implementation. |
| remotesojourner/papra-companion | none | All rights reserved. Treat as specification only: describe behavior, reimplement from scratch. No verbatim or near-verbatim code, not even small functions. |
| koljam/papra-ai | none | Same as papra-companion. The language mismatch (C#/TS vs Python) helps; porting line-by-line is still derivative work and forbidden. |

- Facts (API endpoints, parameter names, documented behavior) are not copyrightable and are freely usable; expression (code, prompts) is not. The OCR transcription prompt required by this spec is trivially functional; the title prompt must be written from this spec's requirements, not lifted from papra-companion's wording.
- This repo must carry its own LICENSE before publishing (MIT or Apache-2.0). Add `.gitignore` covering `.env` in the first commit.

## 11. Relationship to upstream Papra: standalone service, not a fork

- This service stays a separate process talking to Papra's public API. Do not fork Papra. Rationale: upstream is actively absorbing this feature space (native AI-based content extraction, intake emails, MCP roadmap); a fork means permanent merge conflict resolution in a large TypeScript monorepo, while a thin API client only gets smaller as upstream ships native equivalents.
- Consequence for the build: keep each module deletable. Before implementing a module, check current Papra release notes for a native replacement; modules must not develop tentacles into each other or into Papra internals.
- AGPL note: this service links nothing from Papra, talks only HTTP, and therefore has no AGPL obligations. Keep it that way; do not vendor Papra code.

## 12. Agent build instructions

- Milestones, each must end green (tests + lint + run) before the next:
  1. Scaffold, config, store, Papra client with integration test against a scratch Papra container; open the running instance's OpenAPI document and encode the real paths.
  2. Mail poller with a local GreenMail-style IMAP fixture (use `greenmail` or `aiosmtpd` in docker-compose.test.yml), upload pipeline, dedup.
  3. OCR + title workers with a mocked OpenAI-compatible server (fixture: two-page German PDF → expected markdown contains known strings).
  4. Dashboard: auth, status, mailbox CRUD, retry buttons.
  5. MCP entrypoint, tested with the FastMCP in-memory client.
  6. Dockerfile + compose + README with the volume chown note; acceptance run of §8.
- Reference repos (read, borrow patterns, do not vendor):
  - [papra-hq/papra](https://github.com/papra-hq/papra) — the platform itself. Use the [API endpoint docs](https://docs.papra.app/resources/api-endpoints/) as the source of truth for paths and params; read the running instance's OpenAPI document and encode exactly what it exposes.
  - [remotesojourner/papra-companion](https://github.com/remotesojourner/papra-companion) — IMAP download loop, message/attachment handling, title prompt, the processing-delay concept.
  - [koljam/papra-ai](https://github.com/koljam/papra-ai) — PDF page rendering, vision prompt design, content write-back flow, state tracking of processed documents.
  - [Rigare/papra-mcp](https://github.com/Rigare/papra-mcp) — the MCP tool surface for Papra (search, get, list).
  - Background: [Papra AI-based OCR PR #1283](https://github.com/papra-hq/papra/pull/1283) and the [content extraction guide](https://docs.papra.app/guides/content-extraction/) show how upstream thinks content should be written back.
- Constraints for the agents: Python 3.12, stdlib + httpx, imaplib, PyMuPDF, Pillow, FastAPI, FastMCP, itsdangerous; no Celery, no Redis, no message broker; no ORM, plain sqlite3. Single compose service. Tests must not require a real MLX server; mock it.
- Deliverables in the repo: source tree per §4, pinned `requirements.txt`, `Dockerfile`, `docker-compose.yml` + `.env.example`, `README.md` (setup, the volume chown note, how to run the MCP entrypoint), and a test suite runnable with one command.
- Logging: JSON lines to stdout, no secrets ever (IMAP passwords, API keys, and the admin password must be redacted by a single shared filter, not per-callsite discipline); dashboard log view reads the DB ring buffer, not stdout.
- Known pitfalls to encode in the agents' context: the 100.x tailnet IP is not bindable in some setups (userspace tailscaled); Papra blocks webhook-style private URLs, irrelevant here since this service polls the API instead of receiving webhooks; the MLX server needs a vision model served (text-only Qwen will 400 or hallucinate on image parts).
