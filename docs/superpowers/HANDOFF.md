# Handoff — MCP server + search-integrity work

Date: 2026-11-16. Branch `main` @ `327d490`. This file exists because a long session produced
both findings and retractions, and a new session must not re-report the withdrawn claims as
facts.

## Read these first

1. `docs/superpowers/specs/2026-11-16-mcp-search-server-design.md` — the approved MCP design.
2. `docs/superpowers/specs/2026-11-16-ocr-content-search-intermittent-characterisation-design.md`
   — the search-integrity investigation, including what was disproven.
3. `spec.md` §5.6 is superseded by (1): stdio → Streamable HTTP, `download_document_path` gone.

Next step is **implementation plans**, not more design: one plan for the MCP server, one for the
small ingest-visibility changes (below). No implementation before the plans are approved.

**Added the same afternoon, and it outranks the paragraph above.** The content-search
intermittency has a measured cause, and it is not a search defect: see *Two writers, one
column* at the end of this file. Read it before writing or approving either plan — it changes
what the MCP acceptance test is allowed to claim and it reopens nothing else.

## Decisions that are settled (do not reopen without new evidence)

- **Transport/auth**: Streamable HTTP on loopback, exposed with `tailscale serve` (never Funnel).
  Static per-device bearer tokens via the SDK's `TokenVerifier`. No OAuth, no authorization
  server. Clients are **pi** and **Hermes** (operator's own agent) — both send a configured
  `Authorization` header.
- **Secrets**: `sha256(token)` in the `settings` table, never plaintext; `papra-suite mcp-token
  create|list|revoke`; plaintext printed once; create-then-revoke on rotation. No ACLs (operator
  treats the tailnet as private), which is why revocation is per-device and Host/Origin
  validation stays enabled.
- **Allow-list**: `settings` key `mcp_allowed_org_ids` of Papra org ids, validated against Papra's
  own org list, re-read every call, malformed fails closed, valid-but-empty starts locked.
  `papra-suite mcp-orgs` CLI until M4's dashboard edits the same key.
- **Search**: we tokenise the query, strip punctuation/operator characters, run ≤6 single-token
  Papra queries and intersect document ids locally. Never forward prose or field syntax to Papra.
  Empty results are never presented as proof of absence.
- **No rename workaround.** `titles.py` keeps renaming with `PATCH(name)` alone. See *Withdrawn*.
- Also agreed, outside MCP: no backup system, just a README recovery section (Papra's 409 is
  already treated as "already ingested", the sweep re-enqueues, `replay` rewinds cursors);
  per-attachment `ingest_events` rows with 90-day retention for dashboard visibility of skipped
  non-PDFs, denied senders, oversized messages and failed uploads, with attacker-supplied strings
  treated as untrusted data; product scope is documents generally (contracts, personal papers),
  not invoices; drop "(German or English)" from the title prompt.

## Withdrawn claims — must not be repeated as findings

- **"Renaming a document destroys its OCR content search."** False. A 7-variable matrix, 5 paired
  trials each, checked at t+0/+30/+300, produced zero misses and zero content loss. The earlier
  0/5 and "matched then stopped" results were artefacts, most probably of query shape. The
  candidate fix (read-then-`PATCH(name+content)`) is unwritten on purpose. Still false after the
  afternoon's work: a rename-only `PATCH` after settle left content intact at t+1/+10/+30, and
  the real writer is *Two writers, one column* below. It is a different mechanism and it does
  not reinstate this claim.
- **"Papra full-text search over OCR content is verified working."** Overstated twice before it
  was understood. What's true: single bare alphanumeric tokens in content are found reliably;
  multi-word phrases, hyphenated tokens and numbers with punctuation under-match **reproducibly**
  on the pinned image. `list_documents` never returns a `content` field (verified twice), so
  search results cannot carry snippets.
- **"Characters ≈ pages/tokens"**, **"8 orgs is a security boundary"**, and **"one new
  dependency"** (one *direct*; the transitive tail is real) were all wrong and are corrected in
  the specs.
- **`If-Match` / `If-Unmodified-Since` are accepted and ignored** — this one *is* solid and
  reproducible: no `ETag`/`Last-Modified` offered, nonsense preconditions return 200 and apply.
  Optimistic concurrency does not exist; it fails open.

## Still open, explicitly

1. Does a short-lived **per-query cache** in Papra explain the difference between the failed
   probes and the clean matrix (matrix used a fresh phrase per trial)? Untested.
2. If a real archive shows local intersection missing documents, the escalation is owning a
   stemmed FTS index (Kreuzakt's approach: `github.com/anaisbetts/kreuzakt`, not the operator's
   repo — prior art only). Priced, not paid.
3. Titling quality on non-invoice documents: an offline corpus of 5–6 documents read by the
   operator, not a CI test.

## Environment state

- Compose stack up: greenmail healthy, papra up. `PAPRA_REQUIRE_STACK=1` for integration tests.
- Papra pinned by digest in the specs: `sha256:d1f596a3…`, built 2026-10-22. `:latest` in
  `docker-compose.test.yml` should be pinned; pulling returned the same digest, so `:latest`
  carries no version signal.
- Probe scripts (throwaway, `/tmp`): `char_matrix.py` (the 7-variable matrix, log at
  `/tmp/matrix.log`), `char_grammar.py`-style query-shape probes. Re-run before filing anything
  upstream.
- MCP SDK facts measured live, `mcp==2.3.0`: 401/401/200 for none/wrong/right token, 401 for
  query-string tokens, 421 for bad Host, 403 for bad Origin; `token_verifier` requires `auth`;
  `AccessToken` requires `client_id` and `scopes` or the *correct* token 500s; uvicorn logs to
  **stdout** and logs full URLs including query strings (a refused token appeared verbatim).

## Two writers, one column — measured 2026-11-16 afternoon

**Papra extracts text from every uploaded document asynchronously and writes it into the same
`content` column our OCR worker writes.** When our `PATCH(content)` lands before that extraction
finishes, the extraction lands afterwards and replaces what we wrote. The intermittency this
file spent a morning retracted for is a **write** race, not a broken index: the index faithfully
follows whichever value is in the column.

Evidence, on the pinned digest, with a positive control. Upload a `text/plain` file whose body is
`THEIR-EXTRACTED-TEXT-9f3c`, then `PATCH` our own string: 0.2 s later the stored content is the
file's own text, and still is at t+20 s. Paired arms, n=5 each, search token in the content and
nowhere else: `PATCH` at upload+0 s → content length 0 and content-search MISS at every tick from
t+2 to t+60 s, **5/5**; `PATCH` at upload+10 s → HIT at every tick, **5/5**. One variable: when
the write happened. A 10-document burst at upload+0 s lost the write **10/10** and the surviving
text was Papra's own extraction of the file.

The window is the extraction's duration, so **no sleep and no rewrite is safe**. It was 0.24 s
for a two-page text PDF, 1.0 s for a one-page 200-dpi scan, 8.2 s for an eight-page scan, and on
the eight-page scan under a four-way burst, two documents that still held our write at t+20 s had
been overwritten by t+65 s. There is no completion signal: no status field, no `ETag`, nothing to
wait on. Papra's extraction also cannot be turned off — `CONTENT_EXTRACTION_STRATEGY=""` is a
fatal config error, and `internal` is the default, so every deployment has a second writer.

Consequences, in order of importance:

1. **The pipeline can silently lose OCR output.** Our OCR worker normally lands well after the
   extraction, so we win by accident; when we lose, the document carries Tesseract's text instead
   of our markdown, the job is `done`, the `seen` table records it as processed, and nothing
   re-writes it. The sweep never verifies that a content write survived.
2. **This is not in either plan, and must not be folded into the ingest-visibility one.** It
   needs its own short correctness spec with a target, a removal criterion and a conflict policy,
   the same bar this repo set for the rename workaround. Three real options: serve Papra's
   `custom-http` extraction so we *own* the column instead of racing it; reconcile with a bounded
   observation window and a visible failure state; or accept the loss and make it visible in
   `ingest_events`. The operator chooses; nothing in the pipeline may claim a content write is
   durable until then.
3. **The MCP plan's criterion-5 test must not assert at t+0.** A content hit at t+0 is exactly
   the state that can be destroyed. Assert after an observable boundary (content unchanged across
   two reads), report the window waited through, and label it evidence, not a guarantee.
4. **It probably also explains the old 0/5 and "matched then stopped" results**, and the matrix's
   35 clean trials (sub-second window, and every trial did other HTTP work between writing and
   observing). Attribution for those specific probes stays probable — they recorded no timings.

Two things it does **not** touch: the multi-word and punctuation under-matching still needs its
own control arm (`gamma` is not a clean negative control — fitz lays `alpha beta gamma` on one
page, so every text fixture contains all three), and `titles.py` still renames with `PATCH(name)`
alone.

Probes and raw logs: `/tmp/papra-content-extraction-probes/`, numbered 00–07 with `.log` files.

## House rules this session earned

- Measure before claiming; a control arm is not optional; label every claim measured vs inferred.
- Tests must fail under the mutant they target; `-k` filters can select zero tests.
- Fail closed on unknown mail/search responses; never render an unavailable archive as "nothing
  found".
- No new dependencies without saying so; pin exactly; name the transitive tail.
