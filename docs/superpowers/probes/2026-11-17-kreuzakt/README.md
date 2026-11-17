# Kreuzakt contract probe (2026-11-17)

Run against `ghcr.io/anaisbetts/kreuzakt:latest`, keyless (no `OCR_VLM_BASE_URL` / `METADATA_LLM_*`),
amd64 under Rosetta on the MacBook (`--platform linux/amd64`, healthy in ~2s).

    docker run -d --name kreuzakt-probe --platform linux/amd64 \
      -p 127.0.0.1:12400:3000 -e DATA_DIR=/data -v kreuzakt-probe-data:/data \
      ghcr.io/anaisbetts/kreuzakt:latest
    python3 g.py       # upload/dedupe/delete/listing/search/delete
    python3 g2.py      # detail fields, positive search shape, paging caps, since, id reuse

## Findings that changed the design

1. `POST /api/upload` field is `files`; response is
   `{files:[{originalFilename, relativePath, storedFilename}], uploadedCount, failedCount, errors}` —
   **no document id, no hash**. Correlation is `relativePath` → `/api/queue` row on `filename` →
   `document_id` at `status=completed`.
2. Queue rows are keyed by **filename**, and re-uploading the same name leaves several rows with that
   name. So the bridge must upload under a unique deterministic name (`papra-<docId>-<hash8>.pdf`).
3. **Dedupe is many-to-one, live-confirmed:** identical bytes under two filenames → two queue rows,
   one `document_id` (`1`). `DELETE /api/documents/1` → 204, then 404 for both. One bucket's delete
   destroys the other bucket's search hit, hence reference-counted deletes and no unique index on
   `kreuzakt_id`.
4. API fields are **snake_case**; detail carries `download_url`, `link_for_user`, `thumbnail_url` as
   **relative paths** (`/api/documents/2/original`, `/documents/2`, `/api/documents/2/thumbnail`).
   `fileHash` is not exposed at all; it survives only as the 12-hex prefix of `stored_filename`.
5. `/api/search` → `{results, total, page, limit, query}`, `limit` capped at **100**, `offset` honoured.
   Hit = `{id, title, description, content, snippet, document_date, added_at, mime_type,
   original_filename, thumbnail_url}`; **`score` absent, `snippet` empty**. Hits carry full `content`.
6. `/api/documents` supports `page`, `limit` (≤100), and **`since`** (verified: 2020→1, 2030→0).
7. Delete then re-upload → new id (3, not 2). No id reuse within a live DB.
8. Keyless mode extracts a text-layer PDF exactly (`SEPA Überweisung`, umlaut intact) but titles from
   filename (`Papra Doc9001 A1b2c3d4`), `description` empty, `document_date` null → title/description/
   date quality is an omlx measurement, not measurable here.

## Papra webhook surface found while probing (scratch stack, `db.sqlite`)

`webhooks(url, secret, enabled, name, organization_id)`, `webhook_events(webhook_id, event_name)`,
`webhook_deliveries(webhook_id, event_name, request_payload, response_payload, response_status)`;
routes `/api/organizations/:organizationId/webhooks[/:webhookId]` plus an `/api/admin/...` variant;
emitted events include `document.created/updated/deleted/restored/tags.changed`. **Delivery payload,
signature header, retry policy and `webhookUrlAllowedHostnames` behaviour are unmeasured** — the
scratch stack's injected-session auth no longer reproduces, so the live test runs with a key created in
the UI, which production needs anyway.
