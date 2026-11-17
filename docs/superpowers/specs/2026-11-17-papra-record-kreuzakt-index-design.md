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
                                                      converging       │
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

### 4.1 Papra: extraction stays on, and the write converges instead of winning

**Papra's extraction is left enabled.** Turning it off was the plan until the reason not to turned
up in the bundle: auto-tagging (`enrich-document`) is scheduled from exactly one site, inside the
content-extraction job's handler, and its prompt is built from `Document name:` and
`Document content:`. Disabling extraction therefore removes the only thing that ever schedules the
tagging model, and there is no API route to re-run enrichment afterwards — the tagging endpoints are
tag CRUD and `/tagging-rules/:id/apply`, which is the deterministic rule engine. The operator
management model keeps its auto-tags, so the flag stays unset.

What that means for the column, stated plainly: there are two writers. Papra writes its own Tesseract
pass once, at upload; the bridge writes Kreuzakt's text afterwards. They race, and the race no longer
controls findability, because search lives in Kreuzakt now — what it controls is what Papra's own
search box shows, and which text the tagger saw once at upload. `PATCH` triggers neither extraction
nor enrichment: the two schedule sites are document creation and `POST /documents/:id/reprocess`, so
write-back cannot loop and cannot re-tag.

Convergence is the job of the verification pass rather than of ordering. Every six hours, and once at
startup, read each document's detail and `PATCH` any whose content differs from what the ledger says
we delivered, repairing **from the current Kreuzakt content and version** rather than trusting a
stale stored hash; count the repairs in `check`. Divergence is tracked per document
(`repair_count`, `last_repair_at`) over a rolling window, with `papra-suite jobs --reset` as the
operator path back. If one document diverges more than twice in a day, stop repairing it and log at
`ERROR` naming the observable rather than a culprit: `/reprocess`, a manual content edit, a bridge
bug, and a second writer are all consistent with that pattern, and naming the wrong one is how an
unexplainable bug gets made.

With extraction on, `DOCUMENTS_OCR_LANGUAGES` matters: at its default the instance reads German with
English language data and produces `StraBe` and `GriiBe`, which is now not only wrong text in the box
but wrong text fed to the tagger. The operator sets `deu,eng` in his `.env`; the test compose mirrors
it so the tested topology is the deployed one. Note that the variable replaces the default rather than
extending it, and that each language's traineddata is fetched on first use into the container
filesystem — so a recreated container needs outbound internet once per language, unless the file is
bind-mounted.

### 4.2 The bridge

Runs on the store's existing job queue, replacing the `ocr` job kind with `index`: the claim/run/
retry/backoff loop and its crash-ordering tests are already written and stay, while the two LLM
workers go away.

Per document:

**Contract first.** Kreuzakt dedupes by hash, which this design initially treated as a convenience.
It is a change of cardinality: the same bytes in two buckets become **one** Kreuzakt document, so
Papra document → Kreuzakt document is many-to-one, not one-to-one (§7). Before any of this is
planned into code, the upload contract gets measured, because the bridge's idempotency rests on it
and the queue schema holds `filename` and `document_id` but **no hash** — so the obvious recovery
for a lost upload response does not follow from the schema and must not be assumed:

- duplicate upload while the first is still processing;
- duplicate upload after completion, and whether the response carries the existing document id;
- whether a lost upload response can be resolved to a document id through supported APIs at all, and
  if not, whether the bridge must carry its own correlation handle;
- queue retention, and what a `rescan` does to `document_id` and `updated_at`.

Per document:

1. Upload the original bytes, then resolve the Kreuzakt document id by whatever the contract test
   proves is the supported route — the upload response if it carries one, otherwise a lookup that
   exists — and record it in the ledger as a reference (§7).
2. Wait for `status = 'completed'`. **Completion means the pipeline finished, not that the page has
   no text**: an empty `content` may be a blank page, a failed-but-unreported extraction, or the
   keyless mode that skips OCR. So empty text is never written over non-empty Papra text, and it is
   reported as `extraction-empty`, a distinct category from `legitimately-blank`, which nothing can
   actually establish from here. On `failed`, log the queue's `error` and use
   `POST /api/queue/{id}/retry` under the job queue's own attempt ceiling.
4. `PATCH` Papra with `content` when Kreuzakt's `updated_at` is newer than what the ledger says we
   last delivered — so a `rescan` or `reindex-all` propagates rather than being ignored — and skip
   when the text is empty and Papra's is not.
5. Rename per §4.3, separately from content, because the two have different rules.
6. Deletes are reference-counted, and still one-way. Papra is the record, so a Papra deletion
   (`isDeleted`/`deletedAt`, which probe E confirmed in both listing and detail) removes that
   document's **reference**: it stops resolving for every token immediately, and Kreuzakt's `DELETE
   /api/documents/{id}` is issued only when no active reference remains. Deleting one bucket's copy
   of a file that another bucket also holds must not remove the index entry the survivor depends on.
   A Kreuzakt deletion never propagates back; it makes every referencing document unfindable, which
   is honest, and the next sweep re-indexes it.

Papra-native uploads — web UI, intake email — are covered without new machinery: `sweep_once`
already exists to "record and enqueue OCR for externally-ingested documents", so its
enqueue call site becomes an index enqueue and those documents enter the same pipeline.

### 4.3 Titles, and why your renames win in practice rather than by guarantee

The bridge renames Papra documents with Kreuzakt's LLM title, and a rename you made is meant to
survive. Before writing, read Papra's current `name` and rename only if it equals the name the ledger
says we last wrote, or, when there is no ledger row for it, equals the original upload filename —
checkable externally because Kreuzakt stores `original_filename`. Anything else is treated as a human
edit: log once, set `name_protected`, and stop touching `name` for that document, while content
updates continue.

This is a **read-then-write heuristic, not a compare-and-swap**, and the spec should not call it
one. Papra offers no conditional write on `name`, so a rename landing between our read and our write
is overwritten, and a store rebuilt without ledger history cannot distinguish our last title from
yours — which is why the original-filename rule exists as a fallback. The honest claim is: deliberate
renames survive the ordinary path, the window is small, and the worst case is one lost rename rather
than lost text. The same policy statement applies to `content` and is worth saying out loud: **a
manual edit to Papra's content will be overwritten** by the next delivery or verification pass.
Per-reference rename state lives on the ledger row for each Papra document, so two buckets holding
the same bytes protect their own names independently. `--force-names` re-titles deliberately and updates the ledger; a per-document
`names --restore-auto` opts a protected name back in.

### 4.4 Kreuzakt configuration

One container, `DATA_DIR` on a real volume, `OCR_VLM_BASE_URL` and `METADATA_LLM_*` pointed at the
operator's local OpenAI-compatible endpoint, so extraction and titling stay free, and **published on
no host port at all**: Kreuzakt joins a private compose network shared only with the suite container,
and the suite reaches it by service name. Publishing `127.0.0.1:port` would not work for this shape —
the loopback of the published port is the host's, not the suite container's `localhost` — and it would
also put an unauthenticated service on a host port for no benefit. The reachability list is therefore
short and must be written down: the suite container, and nothing else, plus the host if someone
`docker exec`s into the network by hand. Its own UI stays unserved — an unauthenticated archive is not
worth a nicer queue screen, which is why queue visibility arrives as `index_status` and a `check` line
instead. Two
operational notes worth recording: nothing coordinates OCR concurrency across the queue, so a bulk
import is a sustained load on one local VLM and backfills should go one bucket at a time; and
Kreuzakt keeps its own copy of every original, so disk holds each document twice, once per system.

Placement: it joins the operator's Dokploy stack for `papra-suite`, not Papra's stack, so a change to
the archive does not restart the index and vice versa. This repo carries only
`docker-compose.test.yml`, so the production shape is a documented service snippet — pinned by
digest, same discipline as the Papra pin — mirrored by a pinned Kreuzakt service in the test compose
so the tested topology and the deployed one stay the same set of truths.

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
| `index_status` | Kreuzakt queue counts by status, so an agent can say "still being indexed" instead of "not in your archive" |

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

1. Kreuzakt is reachable only from the private compose network the suite shares with it; no host port
   is published. Any client-visible URL in any tool response points at Papra.
2. Every MCP handler resolves through the ledger and filters on the token's buckets, and resolution
   **fails closed**: an unknown, stale, deleted, or unreconciled Kreuzakt id yields nothing rather
   than a best guess. The ledger is an authorization lookup, not a live copy of Papra's permissions,
   so the document's current Papra state is checked on the way out and the reconciliation delay is
   documented rather than hidden.
3. Papra writes `content` once, at upload; the bridge writes it only after Kreuzakt reports
   `completed`, and never empty over non-empty. The column then **converges eventually** — repaired
   from the current Kreuzakt content and version, on a six-hour cycle plus startup — which is a
   cadence, not a bound: an outage or a suppressed repair extends it.
4. A `name` you changed is protected on the ordinary path; the guarantee is best-effort, not atomic
   (§4.3).
5. Deletes propagate Papra→Kreuzakt only, and only when no active reference to that Kreuzakt document
   remains.
6. One Kreuzakt document per distinct file hash; any number of Papra documents may reference it, in
   one bucket or several.
7. A search result the caller is not entitled to see cannot be returned by any tool, under any query,
   in any bucket combination — and a hit is expanded into that caller's **authorized, active** Papra
   references, not into whichever reference the bridge happened to index first.

## 6. Failure and reconciliation

| event | consequence | recovery |
| --- | --- | --- |
| Kreuzakt down at intake | Papra upload already succeeded; index job retries with backoff | nothing to do; sweep re-offers the document if the job is lost |
| Kreuzakt OCR `failed` | queue keeps `error`, document unfindable | `POST /api/queue/{id}/retry`, driven by the job queue's ceiling; then `ERROR` naming the document |
| PATCH to Papra lost after success | Kreuzakt newer than ledger | ledger's last-delivered `updated_at` is behind, so the next pass re-delivers |
| `rescan`/`reindex-all` in Kreuzakt | text changes there | `updated_at` comparison re-delivers |
| Document deleted in Papra | Kreuzakt keeps an orphan, search would hit a missing file | propagate `DELETE`, and the ledger row goes `deleted` so its id can never resolve for any token |
| Document deleted in Kreuzakt | unfindable, Papra unaffected | sweep re-indexes it |
| `POST /documents/:id/reprocess` pressed in Papra | Papra's Tesseract text replaces ours and the tagger re-runs on it; auto-tags refresh, the better text is lost | the six-hour verification pass restores the text; tags stay as the re-run left them, because nothing re-tags on our write |
| Ledger lost | mapping Kreuzakt→Papra gone | rebuild by hash: Papra's listing carries `originalSha256Hash` and Kreuzakt stores `file_hash`, and the bridge knows the hash of everything it uploaded; rename protection degrades to the original-filename rule, which is why that rule exists |
| MCP token leaked | that token's buckets readable, no others | rotate the `settings` row; no Kreuzakt access is gained, because Kreuzakt is loopback-only |

## 7. Data model

One new table, and it carries three jobs: Kreuzakt↔Papra mapping, delivered-text bookkeeping, and
rename protection.

One row per **Papra** document — a reference, not a pairing. Kreuzakt dedupes by hash, so the same
bytes in two buckets yield one Kreuzakt document and two rows that share `kreuzakt_id`; that is why
there is deliberately **no unique index on `kreuzakt_id`** and why resolution returns a set.

```sql
CREATE TABLE index_ledger (
    papra_org_id      TEXT NOT NULL,
    papra_document_id TEXT NOT NULL,
    kreuzakt_id       INTEGER,         -- many rows may share one value, by design
    file_hash         TEXT NOT NULL,
    delivered_at      TEXT,            -- Kreuzakt updated_at we last PATCHed into Papra
    delivered_sha     TEXT,            -- sha of the delivered text, to skip no-op writes
    written_name      TEXT,            -- the name we last wrote, for the rename heuristic
    name_protected    INTEGER NOT NULL DEFAULT 0,
    repair_count      INTEGER NOT NULL DEFAULT 0,
    last_repair_at    TEXT,
    deleted_at        TEXT,
    PRIMARY KEY (papra_org_id, papra_document_id)
);
CREATE INDEX idx_index_ledger_kreuzakt ON index_ledger (kreuzakt_id)
    WHERE kreuzakt_id IS NOT NULL AND deleted_at IS NULL;  -- hit -> active references
CREATE INDEX idx_index_ledger_hash ON index_ledger (file_hash);
```

Three consequences of the reference model, all of them acceptance tests rather than notes. Deleting a
reference excludes it from resolution immediately, and the upstream `DELETE` waits until no active
reference remains. Rename protection and delivery state are per reference, so two buckets holding the
same file can carry different titles and different repair histories. And **a rebuilt Kreuzakt can
reuse integer ids**, so any rebuild or restore invalidates stored `kreuzakt_id` values before new
results are accepted, and re-maps by `file_hash` without discarding `name_protected` or per-reference
delivery state.

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
- **dedup is many-to-one, and deletion is reference-counted**: upload identical bytes into two
  buckets, query with separately scoped tokens so each sees only its own, then delete one copy and
  assert the survivor is still searchable, readable, and correctly titled, and that Kreuzakt was not
  asked to delete the document it still needs. Restore must reconcile the reference back in.
- **unknown and stale ids fail closed**: a Kreuzakt hit whose ledger row is missing, deleted, or
  points at a document Papra reports deleted resolves to nothing for every token.
- **rebuild safety**: wipe Kreuzakt, re-index, and assert the old integer ids cannot resolve to the
  wrong document, while `name_protected` and per-reference delivery state survive.
- over-fetch: hits for other buckets crowding the first Kreuzakt pages still yield a full page for
  the caller; scan cap produces `partial` rather than a short list presented as complete.
- link rewriting: no response contains a Kreuzakt hostname or a Kreuzakt integer id as its
  client-facing identifier.
- ledger rebuild from hashes reproduces the mapping and degrades rename protection to the
  filename rule instead of renaming over a human edit.

Integration, real containers on the tailnet-free ports:

- mail attachment → Papra document → Kreuzakt index → Papra `content` replaced by Kreuzakt's text.
  The wording of the OCR is non-deterministic, so the assertion is that **Papra's content equals the
  text captured from Kreuzakt for that document** — transport fidelity is exact even when the model is
  not — plus ledger advanced and name renamed. Do not assert that the text differs from Papra's own
  pass: with `deu,eng` both can legitimately produce the same correct sentence.
- a simulated late Papra write (write our text, then overwrite it, then run the verification pass) ⇒
  the column converges back, and a document that flip-flops twice is left alone with one `ERROR`.
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
- Whether Kreuzakt's upload contract supports the bridge's assumptions at all: dedupe semantics while
  a first upload is still processing, whether a duplicate response carries the existing document id,
  whether a lost upload response is resolvable through supported APIs (the queue stores `filename` and
  `document_id` but no hash), and queue retention across rescans. §4.2 makes this the first task
  precisely because none of it is verified.
- How many of the operator's 500 documents are the same file in more than one bucket, which decides
  how often the many-to-one reference model actually bites.
- `ghcr.io/anaisbetts/kreuzakt` on Apple Silicon: no `linux/arm64` manifest was offered for the
  `latest` tag when this was checked, and Xberg ships native Node bindings, which is the class of
  dependency emulation breaks. Where the integration stack runs is undecided.
- German quality of Kreuzakt's extraction on the operator's corpus, which §9 leaves unmeasured on
  purpose. The backfill is the first real evidence and the old worker is deleted after it.
- Any Kreuzakt version behaviour beyond the commit read here; it should be pinned by digest the same
  way Papra is, before it is trusted with the index.

## 11. Sequencing

Ordered so that each step's failure is reversible without touching the system of record.

1. **Contract test first** (§4.2): upload, dedupe, duplicate-while-processing, lost response, queue
   retention, rescan behaviour. If resolution by anything other than the upload response turns out to
   be unsupported, the reference model and the idempotency story change here, cheaply, rather than
   after the bridge exists.
2. Ledger, and Kreuzakt deployed on its private network with no published port.
3. **Index without modifying Papra.** The bridge uploads, waits, and records — it does not write.
   Prove retry, duplicate, deletion, restore, and rebuild behaviour while Papra is untouched, because
   this is the last phase where a bridge bug cannot corrupt the record.
4. Enable content delivery alone, and confirm Papra looks right.
5. Enable title delivery separately, so a rename problem is separable from a text problem.
6. MCP authorization, the ledger filter, and bounded paging, with the isolation and fail-closed tests.
7. Backfill bucket by bucket — which also re-extracts the corpus that currently holds Papra's
   mangled German text, so no mangle-survey heuristic is needed — then retire `ocr.py` and
   `titles.py`, and only then.
8. The operator enables auto-tagging per bucket in the Papra UI and sets `deu,eng` in his `.env`; the
   test compose mirrors the language setting. Nothing in our code requests `organizations:update` or
   touches either setting.

A platform caveat that changes step 2 and the test stack: `ghcr.io/anaisbetts/kreuzakt` published no
`linux/arm64` manifest when this was checked, so on Apple Silicon it runs only under emulation, and
Xberg ships native Node bindings which are exactly the kind of dependency emulation breaks. Either
the integration stack runs on the amd64 host that already runs the rest, or the pinned digest is
checked for an arm64 variant before `make test-integration` is asked to depend on it. This is
unresolved, not settled.

## 12. What this supersedes

- `2026-11-17-content-single-writer-design.md`: **superseded outright, including its central
  recommendation to disable Papra's extraction** — see §4.1 for why that flag is now a bad trade.
  Its measurements in §7 stay as evidence, and its `jobs` CLI idea is carried into this plan, because
  `Store.reset_job` having no caller is a property of our store either way. Its recovery pass is not built: the failure mode it was written for — a permanently
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

## 13. Decisions taken in the review round

Recorded because each one was a live choice, and the plan inherits all of them.

| # | decision | settled |
| --- | --- | --- |
| 1 | bridge and MCP listener run inside `serve` | one store writer, one restart; a second process means WAL and lock discipline we never needed |
| 2 | Papra extraction stays **on** | auto-tagging is chained to it and cannot be re-triggered (§4.1); race demoted to cosmetic, repaired by convergence |
| 3 | tags are hand-applied by the operator, auto-tagging enabled by him in the UI | our API keys keep no `organizations:update` |
| 4 | corpus is ~500 documents | a full verification pass is seconds, which is what makes option (b) affordable |
| 5 | two tokens, `pi` and `hermes`, both all buckets | the mechanism stays so a scoped token is a `settings` row; the real boundary is the tailnet ACL plus file permissions |
| 6 | read-only tools, no upload path | Kreuzakt cannot say which bucket an upload belongs to; intake stays mail plus Papra's UI |
| 7 | PDFs only, everywhere | `_is_pdf` gate unchanged |
| 8 | Kreuzakt `/data` disposable, rebuild runbook written and timed | originals and ledger live in Papra and our store, so a rebuild costs VLM minutes |
| 9 | hard error when the index is down, no Papra-backed search fallback | one search authority; "found nothing" must keep meaning "not in your archive" |
| 10 | integration tests use the real local model | assertions become structural; the blank-page rule stays provable only through fake transports |
| 11 | Kreuzakt joins the operator's Dokploy stack for `papra-suite`, not Papra's | documented snippet in this repo, pinned by digest, mirrored in the test compose |
| 12 | `deu,eng` OCR languages, set by the operator | fixes Papra's German outright (probes §6); replaces rather than extends the default |
| 13 | `papra-suite jobs --status/--reset` kept, last task | the one failure mode Kreuzakt did not gain a repair path for |
| 14 | `papra-suite backfill [--org-id] [--limit] [--dry-run]` | the sweep would get there eventually; eventually is a day, and `--dry-run` prices the afternoon |
| 15 | Kreuzakt strictly loopback, its UI never served | an unauthenticated archive is not worth a nicer queue screen; queue counts go in `index_status` and `check` instead |
