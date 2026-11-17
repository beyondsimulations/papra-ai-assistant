# Kreuzakt + Papra contract probe (2026-11-17, Task 3)

Measured against real containers from the Task 2 stack. Source was read afterwards
to explain some of it; where it is quoted it is labelled source, not measurement.

Kreuzakt: `ghcr.io/anaisbetts/kreuzakt@sha256:4a0109d0…`, the digest
`docker-compose.test.yml` pins, keyless (`OCR_VLM_API_KEY` unset — `/api/health`
still prints default model names with no key behind them, so extraction is
text-layer only). Papra: the pinned `ghcr.io/papra-hq/papra@sha256:d1f596a3…`.

    docker compose -f docker-compose.test.yml -p papra-ai-assistant stop
    docker compose -f docker-compose.test.yml -p kreuzakt-bridge up -d --wait
    .venv/bin/python docs/superpowers/probes/2026-11-17-kreuzakt-contracts/probe_queue.py
    .venv/bin/python docs/superpowers/probes/2026-11-17-kreuzakt-contracts/probe_webhooks.py

Raw payloads sit next to this file: `capture-queue.json`, `capture-webhooks.json`,
`capture-webhooks-before-allowlist.json`, `listener.jsonl`. Every row id quoted
below is a real id from the run in `capture-queue.json`. Upload names are in the
shape the bridge sends (`papra-doc0001-1a2b3c4d.pdf`), so this measures the naming
we will actually rely on.

## Q1 — `POST /api/upload` (`Q1a`, `Q1b`, `Q1c`)

201, `{"files":[{originalFilename, relativePath, storedFilename}],
"uploadedCount", "failedCount", "errors"}`. **No document id, no hash** —
correlation still has to go through `/api/queue`. Two files in one request produce
one 201 with both entries in request order (`uploadedCount: 2`) and one queue row
each. Wrong multipart field name → 400
`{"error":"bad_request","message":"Upload requests must include at least one file"}`.

## Q2 — identical bytes twice while the first is **still processing** (`Q2_…`)

First row read mid-flight: `status: "processing"`, `document_id: null`. Second
upload — same bytes, different name, no wait — got **its own new row**: rows 31
and 32, both `completed`, **both `document_id: 16`**. Two rows, one document; the
duplicate path completes rather than failing; the two names collapse onto one
`document_id`.

## Q3 — identical bytes **after completion** (`Q3_…`)

New row (33), new id, **same** `document_id: 16`. Three rows, one document. And
re-uploading the **same filename** once its first attempt had completed produced a
fourth row (34) carrying the *identical* `filename` and the same `document_id` — a
filename can hold several terminal rows, which is why correlation needs `filename`
**plus** `id > high-water mark`, and why two terminal rows for one upload have to
be reported ambiguous rather than resolved by picking one.

## Q4 — accepted parameters, largest limit, walking (`Q4_parameter_matrix`, `Q4_walk`)

Envelope `{"entries": [...], "counts": {pending, processing, completed, failed}}`;
row keys `id, filename, status, error, document_id, created_at, completed_at,
page_count` (the last joined from the document, so it is null when there is no
live document).

| parameter | verdict |
|---|---|
| `limit` | accepted; default **20**; **clamped to 100** — `101`, `500`, `10000` each returned exactly 100 rows, no error; `0`, `-5`, `abc` → 400 `` `limit` must be a positive integer `` |
| `status` | accepted (`pending`/`processing`/`completed`/`failed`); `bogus` → 400 `` `status` must be one of pending, processing, completed, or failed `` |
| `page` | **ignored silently** — `page=2&limit=5` returned the ids of `page=1&limit=5` |
| `offset` | **ignored silently** — `offset=5`/`offset=10` returned the newest rows again |
| `perPage`, `per_page`, `count`, `after`, `cursor` | ignored (200, default 20 rows) |

Largest limit accepted: **100**, by clamping, not by error.

**Walking to empty is impossible.** The page walk stopped at page 2 because the
returned id set was identical to page 1; the offset walk stopped at `offset=100`
for the same reason. With 155 rows in the table the endpoint serves at most the
newest **100** (`Q5_retention`: reachable ids 56…155, `row_id_1_still_reachable:
false`).

Rows versus uploads, exactly: `uploads_accepted_by_this_probe` 140 +
`reindex_rows_created_by_this_probe` 15 = `total_rows_per_counts` **155**. One row
per accepted upload, plus one row per document per `reindex-all` batch; nothing
extra, nothing missing.

`status` is the only second filter and it partitions the same 100-row window: the
four status requests together surfaced **138 of 155** distinct ids, and the rest sat
outside every window at that moment — statuses were still changing, so that is an
observation, not a quiescent proof of permanent unreachability. Correlation consequence
(`Q4_correlation_window`): the probe's own first row (id 1) was **not** reachable
in the default window once 154 newer rows existed, but **was** reachable via
`status=completed`; and `GET /api/queue?limit=1` returned id 155, the highest id,
while 155 rows existed. The trade-off, not a fix: each status window is *itself* capped
at its newest 100, so `status` improves visibility and does not remove the bound.

## Q5 — maximum limit and retention

Cap is 100. **No pruning was observed** during this probe — `counts` grew monotonically
and every status window still showed old ids — which is not the same claim as
indefinite retention; a run this long cannot establish that. What is established is the
mechanism that matters: old rows leave the only window the API has, deleted or not. The
spec's retention worry is therefore not "rows get pruned", it is "the row you still
need has scrolled out of the only window the API has".

## Q6 — what `filename` holds (`Q6_filename_shapes`)

`filename` is the **stored** name, byte-identical to the upload response's
`relativePath`. It is *not* the name we sent:

| sent | stored (= queue `filename`) |
|---|---|
| `papra-doc0004-4d5e6f70.pdf` | unchanged |
| `papra:doc0005<5e6f7081>.pdf` | `papra_doc0005_5e6f7081_.pdf` (unsafe chars replaced) |
| `papra-doc0006-6f708192.pdf` twice, first still in the ingest dir | `…6f708192.pdf`, then `…6f708192 (1).pdf` |

Exact equality is sufficient **against the `relativePath` from the upload
response** — the only value guaranteed to be what Kreuzakt stored. Matching the
name we constructed is not: sanitising and the ` (1)` collision suffix both rename
the row under us. Exact equality is also *required*: reindex rows end with the
original filename
(`reindex-all:<batch>:15:papra-seq0004-9e9e9e9e.pdf`), so any suffix match would
correlate a reindex row as if it were our upload.

## Q7 — `POST /api/queue/{id}/retry` (`Q7a`, `Q7b`, `Q7c`)

* completed row → **400** `{"error":"bad_request","message":"Queue entry 31 is not in failed status"}`
* unknown id → **404** `{"error":"not_found","message":"Queue entry 999999 not found"}`
* failed row (garbage bytes named `.pdf`, error
  `Parsing error: xberg_native_pdf: failed to load bytes: Invalid cross-reference table`)
  → **200** `{"id":35,"filename":"…","status":"pending","message":"Re-queued for processing"}`
  and the visible row-id set is **identical before and after**: the retry **reuses
  the row**, it does not append one. `counts.failed` was 1 before and 1 after — the
  deterministic failure re-failed with the same message, `created_at` unchanged
  (`20:02:41`), `completed_at` moved (`20:02:43`).

So the row we correlated to is the row that gets re-queued, and a deterministic
failure never stops failing: retrying from our side needs a bounded attempt count,
not "retry until it is not failed".

## Q8 — `rescan` and `reindex-all` (`Q8a_rescan`, `Q8b_reindex_all`)

`POST /api/documents/4/rescan` → 200 with the document: same `id`, `updated_at`
`20:02:21 → 20:02:37`, `content` byte-identical (66 chars both), same `title`, same
`stored_filename` (`53c562326a2a_papra-…`). **No queue row was created** (counts and
visible row count identical before and after). In this sample a rescan changed no field
that correlation or write-back depends on — one text-layer document, keyless, so it is
not a guarantee that a rescan rewrites nothing: under an OCR model `content` is exactly
what a rescan rewrites.

`POST /api/reindex-all` → **202** with
`{status: {batchId, total, pending, processing, completed, failed, processed, active, percentComplete}}`,
and it created **one queue row per document** (15 documents → 15 rows) named
`reindex-all:<batchUuid>:<documentId>:<originalFilename>` with **`document_id` set
at creation**, i.e. before the row completes. After the batch: `active: false`,
`percentComplete: 100`, document `updated_at` moved again, `content` unchanged.
Two consequences: a non-null `document_id` does **not** mean a finished row, and
reindex rows consume the same 100-row window our correlation reads.

## Q9 — can `document_id` be null on a `completed` row? **Yes** (`Q9_…`)

`DELETE /api/documents/16` → 204, then `GET` → 404, and all four `completed` rows
that pointed at it (31, 32, 33, 34) still read `status: "completed"` with
`document_id: null` and `page_count: null`. Source agrees: the delete nulls
`document_id` on every queue row that referenced the document instead of removing
the rows. Mind the direction: deletion **can produce** `completed` +
`document_id: null`; the probe does not show every such row came from a deletion. The
converse is unmeasured, and keyless extraction cannot exercise a transient failure that
might do it too. Safe reading: **consistent with deletion, cause unresolved.**

This is the answer Task 6 asked for, and it cuts against the assumption in the
plan: `completed` + `document_id: null` is not a processing anomaly that authorises a
retry. The row alone does not license re-uploading: branch on Papra-side state first —
if the Papra document is gone too there is nothing to re-index, and if it is alive a
re-index is a decision we took, not one the queue row made.

## Q10 — is `id` monotonic, and is the default ordering by id? (`Q10_ordering`, `Q10b_…`)

`ids_in_returned_order: [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]` — **strictly descending**,
and the returned order equals a sort by `id DESC` **and** a sort by
`(created_at DESC, id DESC)`. Both hold because `created_at` is
second-granularity (`datetime('now')`) and non-decreasing in insertion order: 7 of
the 9 adjacent pairs tied, and the three rows created by **one** multi-file request
shared a single `created_at` while ordering inside the tie was `8, 9, 10` — by id.
Source agrees: `ORDER BY queue.created_at DESC, queue.id DESC`.

Monotonicity in the direction the client needs: five sequential uploads produced
row ids `[11, 12, 13, 14, 15]` in upload order — strictly increasing, no gaps, no
reuse (`INTEGER PRIMARY KEY AUTOINCREMENT`).

So, because this decides the correlation rule:

* `id` is monotonic and never reused in a live DB — **sound**;
* ordering is **descending**, so there is no ascending walk, and with Q4 (no
  `offset`) a high-water mark cannot be produced by paging at all;
* the high-water mark is `entries[0].id` of `GET /api/queue?limit=1` — it returned the
  highest id with 155 rows present, newest-100 window or not. That holds because the
  order equals `id DESC` **given non-decreasing `created_at`**; the server's real key is
  `(created_at DESC, id DESC)` and a clock step backwards breaks the equivalence, so
  the `limit=1` mark is sound under observed behaviour, not unconditionally;
* ordering is **not** "`created_at` with unresolved ties", so correlation does not
  need a `(created_at, id)` compound key — the tie-break is `id DESC`.

## Not answerable keyless

* Nothing in Q1–Q10 needed a model, so nothing above is blocked. What stays
  unmeasured is what this task was never going to settle: `description` and
  `document_date` quality against omlx (keyless gave `description: ""` and
  `document_date: null`, as the 2026-11-17 probe already showed), and whether a
  *transient* OCR failure can leave a `completed` row with a null `document_id` —
  keyless extraction of a text-layer PDF cannot fail transiently.
* Whether a backwards clock step could put a row's `created_at` out of order with
  its `id`. Not reproducible here; the ordering claim is measured on this host,
  this run.

## Part C — Papra webhook delivery (reduced scope)

Operator decision 2026-11-17: the receiver (plan Task 10) is **out of v1**, the
six-hour verification/convergence pass covers staleness. Measured here are the two
facts that decide whether a future receiver could ever be correct. **Not measured:
the 500-retry attempt count and interval series, `webhookUrlAllowedHostnames`
behaviour, and receiver robustness (64 KiB etc.) — not measured: no longer gates
any task.** The listener is a plain recorder for the same reason.

Papra's default SSRF protection refuses an in-network receiver, so the run applied
the throwaway override described in `probe_webhooks.py`'s docstring —
`WEBHOOK_URL_ALLOWED_HOSTNAMES=host.docker.internal`, one hostname, every other
control left on. The blanket `WEBHOOK_SSRF_PROTECTION_ENABLED=false` was refused by
policy and was not needed.

### Creating a webhook is not `{"url", "events"}`, and the event names are colons

`{"url": …, "events": ["document.updated"]}` → **400**, verbatim:

```json
{"error":{"message":"Invalid request body","code":"server.invalid_request.body","details":[
 {"path":"name","message":"Invalid key: Expected \"name\" but received undefined"},
 {"path":"events.0","message":"Invalid type: Expected (\"document:created\" | \"document:deleted\" | \"document:updated\" | \"document:tag:added\" | \"document:tag:removed\") but received \"document.updated\""}]}}
```

`name` is required, the create API accepts colon-separated event names only, and **the
accepted enum has no `document:restored` event** (what a restore emits internally was
not measured). A valid create returns 200 with
`{webhook: {id, createdAt, updatedAt, name, url, enabled, createdBy, organizationId}}`.
An unallowlisted receiver hostname → **400**
`{"error":{"message":"The provided URL is not safe to perform requests to","code":"webhooks.ssrf_unsafe_url"}}`
(measured for `hook-not-allowlisted.invalid`, and for `host.docker.internal` before
the allowlist was applied — `capture-webhooks-before-allowlist.json`).

### One real delivery, and what it says

```
{"data": {"documentId": "doc_bm1plswq9gnov2u9jvvc9s6r",
          "name": "Renamed By API Key.pdf",
          "organizationId": "org_srzxqjynfy888x2urlfj1vr3"},
 "timestamp": "2026-11-17T20:10:29.250Z",
 "type": "document:updated"}
```

* **Event name:** top-level `type`. Values are the enum above; the internal event
  names in Papra's own logs are different (`document.updated`, `documents.trashed`)
  and are *not* what the payload carries.
* **Document object:** `data` only — `documentId` and `organizationId`, plus what
  changed: `name` for a rename, `content` for a content write (a second
  `document:updated` arrived carrying `data.content` = the extracted text, from
  Papra's own upload-time OCR), and `createdAt`/`updatedAt` for `document:created`.
  `document:deleted` carries `documentId` + `organizationId` and nothing else.
* **`changes` list on `document:updated`?** **No.** No `changes` key, no field
  list, no before/after. The only signal of *what* changed is which keys `data`
  happens to carry.
* **Attribution — who made the change?** **Absent.** The same rename was made once
  with an API key and once with a browser session; the payloads were structurally
  identical (`{documentId, name, organizationId}`), neither carried a user id, an
  API key id, an actor field or any other discriminator. Papra's own server logs do
  carry it (`authType: "api-key"`, `apiKeyId`, `sessionId`) — it just never reaches
  the event. A receiver therefore cannot tell our `PATCH` from a human edit. That
  is the one fact that would have bounded Task 10, and it says the receiver could
  never have owned that decision; it is moot only because the receiver is out of v1.
* `document:tag:added` was not captured: creating a tag with an API key needs a
  `color` field (400 `Invalid key: Expected "color" but received undefined`) and the
  reduced scope did not need it.

### Signature

Header **`webhook-signature`**, value **`v1,<base64>`**, with `webhook-id: msg_…`
and `webhook-timestamp: <unix seconds>`; `user-agent: papra-webhook-client`.

Reproduced for all five captured deliveries:

```
base64(HMAC_SHA256(secret_as_supplied_bytes,
                   f"{webhook-id}.{webhook-timestamp}.{raw body bytes}"))  ->  "v1," + that
```

with the secret used **verbatim as supplied** (a caller-generated `whsec_`-prefixed
string; the base64-decoded variant did **not** match, and a server-generated secret was
not tested). `hmac.new(secret.encode(), body,
hashlib.sha256).hexdigest()` **does not reproduce it** — `false` on every delivery:
the value is not hex, and the body alone is not the signed string.

## Part D — `/api/search` and `/api/documents` (Task 5 Step 0, 2026-11-17)

Run against a fresh throwaway container (project `kreuzakt-bridge`, the pinned digest,
keyless, empty database), four hand-built PDFs uploaded first — all four contain
`Rechnung` and `Miete`, each carries one unique token (`Ziphoid`, `Quorlund`,
`Vexbold`, `Trumnik`) — so a multi-hit set and a single-hit set were both reachable.
Raw payloads: `capture-search.json`, keys `S0…`/`S1…S7`/`D1…D4`. Script:
`probe_search.py`, which also wrote the fixtures. Afterwards the project was destroyed
with `down -v` and the operator's `papra-ai-assistant` stack was `start`ed again
(`/api/health` → `{"isDatabaseHealthy":true,"isEverythingOk":true,"status":"ok"}`).

### S1–S3 — the two bodies, verbatim

`GET /api/search?q=Ziphoid` → 200:

```json
{"results": [{"id": 1, "title": "Papra Search0001 11111111", "description": "",
  "document_date": null, "added_at": "2026-11-17T22:28:04.906Z",
  "original_filename": "papra-search0001-11111111.pdf", "mime_type": "application/pdf",
  "content": "PAPRA-CONTRACT-PROBE Ziphoid\nRechnung Nr. 4711\nMiete Oktober 2025",
  "thumbnail_url": "/api/documents/1/thumbnail",
  "snippet": "PAPRA-CONTRACT-PROBE <mark>Ziphoid</mark>\nRechnung Nr. 4711\nMiete Oktober 2025"}],
 "total": 1, "page": 1, "limit": 20, "query": "Ziphoid"}
```

`GET /api/search?q=zzqxvbnmt-not-a-word-anywhere` → 200
`{"results": [], "total": 0, "page": 1, "limit": 20, "query": "…"}` — an empty result is
a 200 with `total: 0`, not an error. A search hit therefore carries **`content` and
`snippet`**, and the client strips both.

### S4–S7 — which search parameters do something (`S4_search_parameter_matrix`)

With four matching documents (ids `[4, 3, 2, 1]` at the default `limit` of 20):

| parameter | verdict |
|---|---|
| `q` | the query; the response echoes it as `query` |
| `limit` | **honoured**: `limit=1` → `[4]`, `limit=2` → `[4, 3]`; **clamped to 100**: `limit=101` and `limit=1000` both echoed `"limit": 100`; `limit=0`, `limit=-5`, `limit=abc` → **400** `Pagination parameters must be positive integers` |
| `page` | **honoured** (1-based): `page=2` at `limit=20` → 200 with `results: []` and `total: 4`; `page=2&limit=1` → `[3]`, i.e. the second row; `page=3&limit=2` → `[]`; `page=0`, `page=abc` → the same 400 as above |
| `offset` | **ignored silently**: `offset=1` and `offset=2` each returned `[4, 3, 2, 1]` with the envelope still `page: 1, limit: 20` |
| `pageIndex`, `pageSize` | ignored (Papra's names do not work here): `pageIndex=1&pageSize=1` → `[4, 3, 2, 1]` |

So, unlike `/api/queue`, search **is** pageable — by `page`, not by `offset`. A hit
carries **no score** in any of these responses (`score_field_present: false`), and
`limit` does not change `total`.

### D1–D4 — `/api/documents` (`D2_documents_parameter_matrix`, `D3_…`, `D4_…`)

Envelope `{"documents": […], "total": 4, "page": 1, "limit": 20}`; a row carries
`id, title, description, document_date, added_at, original_filename, mime_type,
thumbnail_url` — **no `content`**, no `stored_filename`, no `created_at`/`updated_at`
(the row cannot show the value `since` filters on). Ordering is newest first.

`limit` honoured and clamped to 100 (`101`/`1000` echo `100`), `0`/`-5`/`abc` → 400
`Pagination parameters must be positive integers`. `page` honoured (`page=2` → 200,
`[]`, `total: 4`). `offset` ignored.

`since` **is** honoured, and this is the trap: `since=2000-01-01T00:00:00Z` → all four
rows, `since=2030-01-01T00:00:00Z` → none, `since=2026-11-17` → all four, and
`since=2026-11-17 22:28:05` → all four. But `since=2026-11-17T22:28:05Z` — the same
instant in ISO-8601, documents included at `22:28:04.906Z`–`22:28:05.723Z` — returned
**200 with zero rows**, as did `since=not-a-date`, while `since=1700000000` returned
all four. The pattern is a lexical string comparison against a `YYYY-MM-DD HH:MM:SS`
column: a value containing `T`, or any string sorting after the stored stamp ("n" >
"2"), silently matches nothing, and a string sorting before it ("1" < "2") matches
everything. **Which column
it filters was not determined** — all four documents were written inside two seconds,
so no value separates them, and the row does not expose the field. Safe reading:
`since` is a coarse gate in the server's own timestamp format, never a watermark, and
an ISO-8601 value must be refused rather than sent.

### What Task 5 encoded from this

`search(query, *, page=1, limit=100)` and `list_documents(*, page=1, limit=100,
since=None)` — `page`/`limit` because they demonstrably work, `offset` because it
demonstrably does not; `limit` clamped client-side to 1..100; a `since` containing `T`
refused with `KreuzaktError` instead of being sent.

## Fixtures (Part B)

`tests/fixtures/kreuzakt_queue_page.json` — the verbatim body of
`GET /api/queue?limit=100` from this run (35 rows, counts
`{pending: 0, processing: 0, completed: 34, failed: 1}`): completed rows with an
id, `completed` rows whose `document_id` the delete above nulled, and a `failed`
row with its error text.

`tests/fixtures/kreuzakt_document.json` — the verbatim body of
`GET /api/documents/15`: keys `id, title, description, content, document_date,
added_at, original_filename, mime_type, thumbnail_url, stored_filename, file_size,
page_count, created_at, updated_at, download_url, link_for_user`.

`probe_queue.py` writes both files itself instead of a human transcribing them, so
a guessed field name cannot survive into the suite. Tasks 5, 6 and 13 build their
fakes from them.

Part D added four more, all written by `probe_search.py` from the live bodies it
captured, and four extracted by a one-line script from `capture-queue.json` (never
retyped):

| fixture | source |
|---|---|
| `tests/fixtures/kreuzakt_search_page.json` | `S1_search_one_hit` — a real one-hit search body, `total: 1` |
| `tests/fixtures/kreuzakt_search_empty.json` | `S2_search_no_hits` — a real zero-hit search body |
| `tests/fixtures/kreuzakt_documents_page.json` | `D1_documents_default` — four list rows |
| `tests/fixtures/kreuzakt_upload_response.json` | `S3_upload_response_verbatim` |
| `tests/fixtures/kreuzakt_upload_rejected.json` | `capture-queue.json` → `Q1c_upload_wrong_field_name.body` |
| `tests/fixtures/kreuzakt_retry_not_failed.json` | `capture-queue.json` → `Q7a_retry_a_completed_row.response.body` |
| `tests/fixtures/kreuzakt_retry_unknown.json` | `capture-queue.json` → `Q7c_retry_unknown_id.body` |
| `tests/fixtures/kreuzakt_retry_requeued.json` | `capture-queue.json` → `Q7b_retry_a_failed_row.response.body` |
| `tests/fixtures/kreuzakt_health.json` | `capture-queue.json` → `Q0_health` |

## Files

| file | what it is |
|---|---|
| `probe_queue.py` | the Kreuzakt probe; writes `capture-queue.json` and the two fixtures |
| `probe_search.py` | the Task 5 Step 0 probe (`/api/search`, `/api/documents`); writes `capture-search.json` and the four Part D fixtures |
| `probe_webhooks.py` | the Papra probe; starts `listener.py` itself if nothing answers on `:8099` |
| `listener.py` | stdlib receiver, one JSON object per line in `listener.jsonl` (`path`, `headers`, `body`, `raw_body`) |
| `make_pdf.py` | hand-built one-page PDFs with a text layer, distinct bytes per marker so dedupe cannot collapse the sample |
| `run_webhook_probe.sh` | clean-log wrapper for `probe_webhooks.py` |
| `capture-*.json`, `listener.jsonl` | the raw payloads every answer above quotes |
