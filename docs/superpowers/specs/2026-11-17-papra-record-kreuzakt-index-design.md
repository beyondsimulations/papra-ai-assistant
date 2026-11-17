# Papra as the record, Kreuzakt as the index — search and MCP design

Date: 2026-11-17
Status: proposed. Kreuzakt's extraction is adopted without a pre-flight quality gate (§9): the
operator ruled the comparison out, so that risk is carried openly rather than measured away.
Decisions this implements: Papra stays the system of record (organisations, tags, management UI);
Kreuzakt becomes the extractor, the search index and the MCP source; the operator's four
organisations are buckets of his own documents, not separate parties.
Evidence: `docs/superpowers/probes/2026-11-16-custom-http/` (Papra-side measurements) and the
Kreuzakt source at `github.com/anaisbetts/kreuzakt` (read directly; every claim about it below
carries the file it came from).

## 1. The topology

```
                      ┌────────────── papra-suite (Python) ──────────────┐
 mail ── poller ──► dedup + store ──► upload ──► PAPRA  (record: orgs,    │
                      │                            tags, names, files)   │
                      │                                                  │
                      └── bridge ──► upload ──► KREUZAKT (extract +      │
                                     poll queue ◄──  index, loopback     │
                                     fetch content ──► PATCH back ───────┤
                                                      only writer        │
 MCP client ──(tailnet)──► our MCP server ──(loopback)──► Kreuzakt REST  │
                             token → bucket filter → Papra links         │
                      └──────────────────────────────────────────────────┘
```

Papra holds the originals, the organisation structure, tags and names, and a copy of the extracted
text so its own search box is usable. Kreuzakt holds its own copy of each original plus the stemmed
FTS index, and is never reachable from outside the host. Our suite remains the only process that
writes to either system.

## 2. Why this split rather than fixing Papra's search

The Papra-side facts are measured, and none of them is a bug I can fix from our side:

- Papra's default extraction is Tesseract with `eng.traineddata` only. `Die Wäsche Straße Grüße Café
  Köln` was stored as `Die Wasche StraBe GriiBe Café Kdln`, and content search then missed `Straße`,
  `Grüße` and `Köln` (`probe_a2_harder_inputs.py`). The `ß` mangle differed on every run — `StraBe`,
  `StraB3e`, `StraBBe` — so it cannot be compensated at query time.
- Papra does prefix matching on raw tokens with no stemming, so inflection is not covered.
- Papra exposes no extraction-status field, which is what turned the two-writer race into an
  undetectable one: our `PATCH` at upload+0 s was overwritten at ~t+2 s, permanently, and a content
  search still hit because Papra's own OCR had read the token off the page (`probe_c2_exact.py`).

Kreuzakt, read in source rather than inferred from its README:

- `src/lib/fts.ts` indexes stemmed `title`, `description`, `content` and `original_filename`, with
  `src/lib/stemming.ts` mapping `de → "german"` — Snowball stemming per detected language. This is
  the capability the corpus needs and Papra cannot be given.
- `src/lib/db/schema.ts` defines `processing_queue(status ∈ pending|processing|completed|failed,
  error, document_id, completed_at)` — the completion signal Papra never had, plus retry from
  `failed`.
- The same file gives `documents.file_hash`, and `documents.content/title/description/
  document_date/language` covers what our `ocr.py` and `titles.py` produce today.
- `src/app/api/documents/[id]/route.ts` implements `GET` and `DELETE` only: **there is no way to
  hand Kreuzakt text we produced ourselves**, so the direction of integration is forced — Kreuzakt
  extracts, we consume.
- No webhooks and no outbound calls exist anywhere in `src`, so nothing pushes to Papra by itself;
  the transport is our pull loop.

## 3. One Kreuzakt instance, and what enforces the bucket boundary

One instance serves all four buckets. Kreuzakt has no organisation concept at all, so the boundary
cannot live there; it lives in our MCP server, which owns the mapping because it is the component
that uploaded every file.

Three rules make that defensible, and they are the design, not caveats:

1. **Kreuzakt binds to loopback and is never exposed** — no tailnet name, no port on a shared
   interface. Its MCP endpoint has no authentication (grepping `src/app/mcp/route.ts` and
   `src/lib/mcp/server.ts` for any credential check returns nothing), so an exposed Kreuzakt means
   an unauthenticated archive. The proxy is the only path in.
2. **Every handler filters on the ledger**, not just search: fetch-by-id, content-by-id, list-recent,
   download-link. A handler that forgets is a cross-bucket leak, so the filter sits in one shared
   resolution helper that returns a Papra-side view or nothing, and a test drives every tool with
   documents from all four buckets and asserts a single-bucket token never sees another's ids or
   text.
3. **Search over-fetches and pages locally.** `/api/search` takes `q`, `page`, `limit` (max 100) and
   ranks by BM25 across the whole corpus, so filtering after the fact can leave page one nearly
   empty when another bucket dominates. The proxy pulls successive Kreuzakt pages until the
   caller's page is full or a scan cap is hit (default 5 pages / 500 documents), preserves BM25
   order — which stays valid inside a filtered subset — and marks the response partial when it hits
   the cap rather than pretending the bucket is thin.

What one instance costs, named so nobody discovers it later: the shared queue means a large backfill
in one bucket delays indexing in the others and `reindex-all` is instance-wide; isolation is a code
property maintained by review and tests rather than by construction; and Kreuzakt's own web UI, if
it is ever opened, shows all four buckets at once because it has no notion of a bucket. Since the
buckets are the operator's own documents, those costs are accepted deliberately here. If a bucket
ever becomes a second party, this section is the one that has to change, and the change is "run
another instance and point that token's route at it" — nothing else in the design moves.

## 4. Components

### 4.1 Papra: extraction off, one writer

`DOCUMENTS_CONTENT_EXTRACTION_ENABLED=false`, set in production and in `docker-compose.test.yml` so
tests exercise the deployed topology. An integration test asserts the behaviour rather than the
variable — upload, do not `PATCH`, poll 15 s, assert `content` stayed empty — because a renamed
variable, a typo, or an upstream default flip all leave the racing writer silently back in place.

Our bridge is then the only writer of `content`, which is the property that made the race a bug in
the first place. `PATCH` lands whatever Kreuzakt produced.

### 4.2 The bridge

Runs on the store's existing job queue, replacing the `ocr` job kind with `index`: the claim/run/
retry/backoff loop and its crash-ordering tests are already written and stay, while the two LLM
workers go away.

Per document:

1. Upload the original bytes to Kreuzakt. Kreuzakt dedupes by hash during ingest, so a retry does not
   create a second copy — the exact behaviour to confirm first in the spike (§9), because idempotent
   retries are what the rest of this assumes.
2. Resolve the Kreuzakt document id through the queue: `processing_queue.document_id` is populated on
   success, so `hash → queue entry → document_id` works even if the upload response is lost. Record
   the mapping in the ledger (§8).
3. Wait for `status = 'completed'`. Emptiness is never the predicate: a blank page legitimately
   completes with `content = ''`, and PATCHing that would wipe Papra. On `failed`, log the queue's
   `error` and use `POST /api/queue/{id}/retry` under the job queue's own attempt ceiling.
4. `PATCH` Papra with `content` when Kreuzakt's `updated_at` is newer than what the ledger says we
   last delivered — so a `rescan` or `reindex-all` propagates rather than being ignored — and skip
   when the text is empty and Papra's is not.
5. Rename per §4.3, separately from content, because the two have different rules.
6. Deletes go one way. Papra is the record: a Papra deletion (soft-delete is visible as
   `isDeleted`/`deletedAt`, which probe E confirmed in both listing and detail) propagates to
   `DELETE /api/documents/{id}`. A Kreuzakt deletion never propagates back; it makes the document
   unfindable, which is honest, and the next sweep re-indexes it.

Papra-native uploads — web UI, intake email — are covered without new machinery: `sweep_once`
already exists to "record and enqueue OCR for externally-ingested documents", so its
enqueue call site becomes an index enqueue and those documents enter the same pipeline.

### 4.3 Titles, and never overwriting a rename

The bridge renames Papra documents with Kreuzakt's LLM title, but a human rename wins permanently.
Before writing, read Papra's current `name` and rename only if it equals the name the ledger says we
last wrote, or, when there is no ledger row for it, equals the original upload filename — checkable
externally because Kreuzakt stores `original_filename`. Anything else is a human edit: log once and
stop touching `name` for that document forever, while content updates continue.

Papra offers no compare-and-set on `name`, so a rename landing between our read and our write can
still be clobbered; that is a best-effort boundary, and its worst case is one lost rename rather
than lost text. `--force-names` re-titles deliberately and updates the ledger; a per-document
`names --restore-auto` opts a protected name back in.

### 4.4 Kreuzakt configuration

One container, `DATA_DIR` on a real volume, `OCR_VLM_BASE_URL` and `METADATA_LLM_*` pointed at the
operator's local OpenAI-compatible endpoint, so extraction and titling stay free, and bound to
loopback. Two operational notes worth recording: nothing coordinates OCR concurrency across the
queue, so a bulk import is a sustained load on one local VLM and backfills should go one bucket at a
time; and Kreuzakt keeps its own copy of every original, so disk holds each document twice, once per
system.

### 4.5 MCP server

The authentication and transport half of `2026-11-16-mcp-search-server-design.md` is kept verbatim:
sha256-hashed bearer tokens in `settings`, the organisation allow-list, the loopback `Host`
allow-list that answers `421` to a bare host, no Funnel, Streamable HTTP session handling, pydantic
tool returns, the error model, and untrusted-content handling for text that originated in a mail
attachment. What is replaced is everything that searched Papra — the tokenisation, the paging
intersection, the search-grammar workarounds — which was the fiddly part and no longer exists.

It is a small MCP **server**, not a pass-through proxy of Kreuzakt's `/mcp`. MCP is stateful
(`initialize`, `Mcp-Session-Id`, streamed responses), so a pass-through means owning session affinity
and inheriting Kreuzakt's tool list as-is, including `get_upload_command` write exposure and
Kreuzakt's integer ids as the client-visible identity. Five tools call Kreuzakt's REST API for the
token's allowed buckets and return Papra-side identities and links:

| tool | behaviour |
| --- | --- |
| `search_documents` | Kreuzakt `/api/search`, over-fetched and filtered by ledger bucket membership, BM25 order kept, `partial` when the scan cap binds |
| `get_document` | by Papra id, resolved through the ledger; 404-shaped tool error if not in this token's buckets |
| `get_document_content` | same resolution, text from Kreuzakt, size-capped as the existing spec prescribes |
| `list_recent_documents` | Kreuzakt ordering, ledger-filtered |
| `open_document` | a **Papra** viewer and download URL, built by us |

That last row is not decoration. Kreuzakt's own tools hand back absolute URLs into Kreuzakt, which is
loopback-only and unauthenticated: a client that followed one would either fail or reach a host it
was never meant to address. Every link the proxy returns is resolved through
`(kreuzakt_document_id → papra_document_id)` and built against Papra, so the client's world is the
system of record and Kreuzakt stays an implementation detail it never learns the name of.

### 4.6 Deleted from our codebase

`papra_suite/ocr.py` and `papra_suite/titles.py` with their tests, the OCR and title worker loops and
their LLM-client wiring, and the `ocr`/`title` job kinds. Kept: the poller, the sender allow-list,
dedup, the sweep, `ingest_events` and its CLI, the store and job queue, `check`, and the Makefile
gates. Nothing about mail ingestion changes.

## 5. Invariants

1. Kreuzakt is reachable from the host only. Any client-visible URL in any tool response points at
   Papra.
2. Every MCP handler resolves through the ledger and filters on the token's buckets; there is no
   handler-local query path.
3. Exactly one writer of Papra's `content`, and its writes are gated on Kreuzakt queue completion,
   never on text emptiness.
4. A `name` a human changed is never written again by us.
5. Deletes propagate Papra→Kreuzakt only.
6. Every uploaded file has exactly one Kreuzakt document, provable by hash, after retries.
7. A search result the caller is not entitled to see cannot be returned by any tool, under any
   query, in any bucket combination.

## 6. Failure and reconciliation

| event | consequence | recovery |
| --- | --- | --- |
| Kreuzakt down at intake | Papra upload already succeeded; index job retries with backoff | nothing to do; sweep re-offers the document if the job is lost |
| Kreuzakt OCR `failed` | queue keeps `error`, document unfindable | `POST /api/queue/{id}/retry`, driven by the job queue's ceiling; then `ERROR` naming the document |
| PATCH to Papra lost after success | Kreuzakt newer than ledger | ledger's last-delivered `updated_at` is behind, so the next pass re-delivers |
| `rescan`/`reindex-all` in Kreuzakt | text changes there | `updated_at` comparison re-delivers |
| Document deleted in Papra | Kreuzakt keeps an orphan, search would hit a missing file | propagate `DELETE`, and the ledger row goes `deleted` so its id can never resolve for any token |
| Document deleted in Kreuzakt | unfindable, Papra unaffected | sweep re-indexes it |
| Ledger lost | mapping Kreuzakt→Papra gone | rebuild by hash: Papra's listing carries `originalSha256Hash` and Kreuzakt stores `file_hash`, and the bridge knows the hash of everything it uploaded; rename protection degrades to the original-filename rule, which is why that rule exists |
| MCP token leaked | that token's buckets readable, no others | rotate the `settings` row; no Kreuzakt access is gained, because Kreuzakt is loopback-only |

## 7. Data model

One new table, and it carries three jobs: Kreuzakt↔Papra mapping, delivered-text bookkeeping, and
rename protection.

```sql
CREATE TABLE index_ledger (
    papra_org_id      TEXT NOT NULL,
    papra_document_id TEXT NOT NULL,
    kreuzakt_id       INTEGER,
    file_hash         TEXT NOT NULL,
    delivered_at      TEXT,            -- Kreuzakt updated_at we last PATCHed into Papra
    delivered_sha     TEXT,            -- sha of the delivered text, to skip no-op writes
    written_name      TEXT,            -- the name we last wrote, for rename CAS
    name_protected    INTEGER NOT NULL DEFAULT 0,
    deleted_at        TEXT,
    PRIMARY KEY (papra_org_id, papra_document_id)
);
CREATE UNIQUE INDEX idx_index_ledger_kreuzakt ON index_ledger (kreuzakt_id)
    WHERE kreuzakt_id IS NOT NULL;   -- resolution by Kreuzakt id must be single-valued
CREATE INDEX idx_index_ledger_hash ON index_ledger (file_hash);
```

Numbering note: `ingest-visibility` already adds migration 002, so this is **003** and lands after
that merge, or is renumbered if the operator prefers the other order. Same lesson as that branch:
the migration test upgrades a copy of a real database and asserts `user_version` advances.

## 8. Test plan

Unit (fake Kreuzakt and fake Papra transports):

- happy path: upload → queue `completed` → `PATCH` content → ledger row with `delivered_at`,
  `delivered_sha`, `written_name`.
- queue `failed` ⇒ job retried with the queue's error text; ceiling reached ⇒ one `ERROR` naming the
  document, no further retries.
- queue `completed` with empty content and non-empty Papra content ⇒ no `PATCH`, and no wipe.
- second pass with unchanged Kreuzakt `updated_at` ⇒ no `PATCH` at all.
- `rescan` (advanced `updated_at`) ⇒ re-`PATCH`, ledger advances.
- rename CAS: our name → renamed; human name → untouched, `name_protected` set, logged once; no
  ledger row and name equals original filename → renamed; `--force-names` overrides protection.
- Papra delete → Kreuzakt `DELETE` issued, ledger marked `deleted`, resolution returns nothing for
  every token afterwards.
- Kreuzakt delete → Papra untouched, sweep re-indexes.
- **bucket isolation, the one that matters**: four documents in four buckets, tokens scoped to one
  bucket each, every tool driven; assert no cross-bucket id, title, text, or URL appears in any
  response, including via `list_recent_documents` and search paging.
- over-fetch: hits for other buckets crowding the first Kreuzakt pages still yield a full page for
  the caller; scan cap produces `partial` rather than a short list presented as complete.
- link rewriting: no response contains a Kreuzakt hostname or a Kreuzakt integer id as its
  client-facing identifier.
- ledger rebuild from hashes reproduces the mapping and degrades rename protection to the
  filename rule instead of renaming over a human edit.

Integration, real containers on the tailnet-free ports:

- mail attachment → Papra document → Kreuzakt index → Papra `content` non-empty and equal to
  Kreuzakt's, `PATCH`-time ordering irrelevant because Papra's extraction is off (the 15 s
  no-`PATCH` assertion stays as the flag's proof).
- a German scan through the whole chain, then `/api/search` for `Straße` and for an inflected form
  (`Rechnungen`-class), asserting the document is found — the exact query class Papra's index could
  not serve, now asserted end to end.
- restart the suite mid-index and confirm the job queue resumes without duplicating the Kreuzakt
  document, checked by hash.

Gate: `make lint`, `make test-unit`, `PAPRA_REQUIRE_STACK=1 make test-integration`, plus a Kreuzakt
stack target in the Makefile (pinned by digest, same discipline as the Papra pin, with the refresh
procedure commented alongside it). Mutation checks: delete the ledger filter, delete the emptiness
guard, delete the rename CAS read — each must fail a named test.

## 9. Extraction quality: adopted unmeasured, and where the risk lands

No comparison between Kreuzakt's extraction and our current worker will be run. That was considered
and declined: Kreuzakt's author is German and dogfoods the tool on German documents, Xberg extracts
with a vision-language model rather than Tesseract, and it runs against the same local model our
worker uses. That is a reasonable basis for a personal archive, and it is still not a measurement —
so this section records what is assumed rather than pretending it was checked.

What is assumed: that a VLM-through-Xberg transcription of a German invoice preserves `ß`, umlauts,
`1.234,56` amounts, IBANs and names as reliably as our own prompt does. The whole reason this
architecture exists is that the wrong extractor makes documents unfindable in ways the reader never
sees, so the assumption is load-bearing on exactly the axis we have been measuring all week.

What makes it survivable rather than reckless:

- **Order of operations.** `ocr.py` and `titles.py` are deleted after the first backfill, not before
  (§11). The backfill is the first time Kreuzakt's German output is produced at scale, and Papra now
  displays it, so the operator reads the new extractor's work in the UI he already uses, with the
  old worker still in the tree if it disappoints.
- **A cheap signal, no new machinery.** During the backfill, `check` reports the count of indexed
  documents whose extracted text came back empty. A VLM that fails on a page tends to fail by
  returning nothing, so that one number is the difference between "a few odd pages" and "the
  extractor is wrong for this corpus".
- **The exit stays open and is cheap.** Originals live in Papra and the ledger maps by hash, so any
  other extractor can be re-fed from the record without losing anything. The fallback — Kreuzakt
  indexes, our worker keeps writing Papra's text — remains available, and it is now one flag rather
  than a redesign; its cost is that Papra and the index may disagree about what a document says.

What is also confirmed on the way, because the bridge depends on it and only the running system can
say it: `POST /api/upload` dedupes by hash on retry, and `/api/search` response shapes.

## 10. Not established

- Upload dedupe behaviour (§9's last bullet) and the exact shape of `/api/search` responses — both
  read from source and reasoning so far, not from a running Kreuzakt.
- Kreuzakt's behaviour with a corpus of a few thousand documents and its paging depth once the
  proxy's over-fetch starts costing round trips.
- Upstream durability: this makes a young single-maintainer project the extractor and the index. The
  exit is the ledger plus hashes — originals can be re-fed to any other extractor, and Papra keeps a
  copy of the text — but the exit is a re-index, not a switch.
- German quality of Kreuzakt's extraction on the operator's corpus, which §9 leaves unmeasured on
  purpose. The backfill is the first real evidence and the old worker is deleted after it.
- Any Kreuzakt version behaviour beyond the commit read here; it should be pinned by digest the same
  way Papra is, before it is trusted with the index.

## 11. Sequencing

1. Merge `ingest-visibility`, then `ocr-prompt-language`.
2. Run Kreuzakt locally and feed it the existing Papra corpus bucket by bucket. That pass doubles as
   the observation window §9 describes, and it also settles what the previous spec left as an
   operator decision: the corpus already holding Papra's mangled German text gets re-extracted here
   and the result written into Papra, so no mangle-survey heuristic is needed once the extractor is
   replaced.
3. Ledger and bridge on the job queue; `ocr.py` and `titles.py` deleted after that pass, not before
   it.
4. MCP server: auth and transport from the existing spec, five tools over Kreuzakt, bucket filter,
   Papra link rewriting.
5. Papra's extraction flag last, on both compose files, with its integration assertion — by then
   Papra's `content` has a writer that does not depend on Papra.

## 12. What this supersedes

- `2026-11-17-content-single-writer-design.md`: **superseded**, except §5.1 (the flag, the test that
  asserts the behaviour, and setting it in the test compose) and the measurements in §7, which stay
  as evidence. Its recovery pass is not built: the failure mode it was written for — a permanently
  failed OCR job with no rediscovery path — moves into Kreuzakt's queue, which has statuses, an
  error, and a retry endpoint. `Store.reset_job` still has no caller; the `jobs` CLI proposed there is
  worth keeping from that document, because a failed job with no operator repair path is a property
  of our store either way.
- `2026-11-16-mcp-search-server-design.md`: **kept** for purpose and scope, process and
  configuration, transport and authentication, organisation allow-list, untrusted content, error
  model, dependencies, validation approach, operations. **Replaced** for the tool implementations,
  cross-org merging and fan-out (now Kreuzakt paging plus a ledger filter, §4.5), and anything whose
  correctness depended on Papra's search grammar.
- The Papra search-grammar probes keep their value as the *reason* for this document; none of them
  describes code we still intend to write.
