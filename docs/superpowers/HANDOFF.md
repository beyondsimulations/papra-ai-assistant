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
  candidate fix (read-then-`PATCH(name+content)`) is unwritten on purpose.
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

## House rules this session earned

- Measure before claiming; a control arm is not optional; label every claim measured vs inferred.
- Tests must fail under the mutant they target; `-k` filters can select zero tests.
- Fail closed on unknown mail/search responses; never render an unavailable archive as "nothing
  found".
- No new dependencies without saying so; pin exactly; name the transitive tail.
