# MCP search server — design

Date: 2026-11-16
Status: design approved by Tobias; supersedes `spec.md` §5.6.
Open dependency: `2026-11-16-ocr-content-search-intermittent-characterisation-design.md`.
Content search was observed failing in some probes and working in a controlled one; until that
is characterised, search reliability is an unknown rather than a fixed defect.

## Purpose and scope

Expose the Papra archive to MCP clients (an agent on another machine, reached over Tailscale)
as a **read-only** search-and-read surface. No tool modifies Papra.

This replaces `spec.md` §5.6, which specified a stdio server and a
`download_document_path` tool. Both change:

- **stdio is wrong for this deployment.** An stdio server is a child process of its client;
  the client is on a laptop, Papra is on a remote host. Transport becomes Streamable HTTP.
- **`download_document_path` is dropped.** A path written on the Papra host means nothing to
  a remote client. Fetching an original file is what Papra's own UI is for.

`spec.md` acceptance criterion 5 — "find a document by a phrase that only occurs in
OCR-written content" — is the contract this design is built to satisfy.

## Process and configuration

`papra-suite mcp` is a new command and a new **process**, not mounted inside
`papra-suite serve`. The pipeline's crash-restart loops, OCR workers, and an agent-facing
request path have no business sharing a process.

> **Decided differently, 2026-11-19 (option C).** The listener ships inside `serve`, and the operator chose
> **one process with objects that cannot write** over splitting the process (option B) or accepting the shared
> handles as they were (option A). The paragraph above stays as the argument that was not taken, so the change is
> legible rather than silent.
>
> What C commits to, and how each part is checked:
>
> - Everything reachable from a tool call gets capabilities with **no write surface**: a store connection under
>   `PRAGMA query_only=ON` that never runs a migration, and Papra/Kreuzakt views whose method sets contain no
>   `update_document`, `delete_document`, `retag_document`, `create_organization`, no POST, PATCH or DELETE —
>   absent from the object, not merely unused by the current handler bodies.
> - Handlers take those view types as dependencies, so a handler that reaches for a write fails to construct
>   instead of failing review.
> - The one legitimate write on the request path, `mcp_tokens.last_used_at`, stays on its own short-lived
>   connection behind the existing 60-second throttle and is never handed to a handler.
> - Not `mode=ro`: the ledger is WAL (`journal_mode=WAL`), and a read-only URI open of a WAL database is fragile —
>   it needs a usable `-shm` and fails where it would have to create a WAL index. `query_only` is a property of the
>   connection, so it works against the live database while `serve` holds it.
> - The claim stays narrow on purpose: this is not a boundary against an already-compromised process, and it is
>   not tenancy. Its promise is that code reached from a tool call cannot mutate Papra or the ledger, and a test
>   says so. Option B becomes the right answer the day a second consumer or a second human appears; it would also
>   isolate listener availability from scheduler availability.
> - Availability, same decision: when the listener task dies inside `serve`, the process must exit **non-zero**.
>   Exiting 0 means a Dokploy `restart: on-failure` policy never fires and a dead port stays silent.

| Variable | Meaning | Required |
|---|---|---|
| `PAPRA_BASE_URL` | Papra API base | yes |
| `PAPRA_API_KEY` | **read-only-scoped** Papra key | yes |
| `PAPRA_MCP_TOKEN` | bearer token clients present, ≥32 chars | yes |
| `PAPRA_MCP_URL` | public URL of this server; drives Host/Origin allow-lists | yes |
| `PAPRA_MCP_HOST` / `PAPRA_MCP_PORT` | bind address, default `127.0.0.1:8100` | no |
| `DATA_DIR` | location of `state.db`, read-only here | yes |

Schema ownership stays with the pipeline: `papra-suite serve` and the CLI migrate, **MCP never
does**. MCP opens the existing database read-only and fails if the schema is unrecognised, so a
fresh `DATA_DIR` is a startup error rather than a silently empty archive.

Two values in the table above are not what shipped, recorded so the next reader does not implement the spec
instead of the product: the default bind is **`127.0.0.1:8765`**, not `:8100`; and the bearer secret is not
`PAPRA_MCP_TOKEN` at all — tokens live in the `mcp_tokens` table, hashed, each labelled and scoped to a list of
organisation ids (`papra-suite mcp-token --create --label … --org-id …`), which is what makes per-bucket scoping
and revocation without a redeploy possible.

Missing or insufficient configuration means **refuse to start**, name the variable, exit
non-zero — including a missing `DATA_DIR`, a malformed allow-list, and a short or absent token.
A **valid but empty** allow-list is different: the server starts and serves nothing, because
"operator has not allowed any organisation yet" is a real state that must be visible rather
than fatal. Malformed is fatal; empty is locked.

## Transport and authentication

Streamable HTTP via `mcp` 2.x. `token_verifier` requires `auth` settings; passing one without
the other raises at construction.

Measured on `mcp==2.3.0`, live server, throwaway environment:

| request | result |
|---|---|
| no `Authorization` | 401 |
| wrong token | 401 |
| correct token | 200, session created |
| token in `?access_token=` | **401 — refused** |
| spoofed `Host` | **421** |
| `Origin: http://evil.example.com` | **403** |
| `GET /.well-known/oauth-protected-resource` | 200, `"bearer_methods_supported":["header"]` |
| `GET /.well-known/oauth-authorization-server` | 404 |

Authentication and Host/Origin validation are provided by the SDK
(`TransportSecuritySettings`, `BearerAuthBackend`, `TokenVerifier`). Our code is one
`verify_token` doing a constant-time compare — we do not hand-roll a security control.

Two SDK details that fail loudly if got wrong, both verified in v2.3.0 source:

- `AccessToken` **requires** `client_id` and `scopes`, not just the token. A verifier returning
  `AccessToken(token=t)` raises inside `verify_token`, so the *correct* token answers 500 while
  wrong tokens look fine. Assert the success path in a test, not just the rejections.
- `validate_token_resource` left unset emits a deprecation warning and behaves as `False`; it
  becomes `True` in 3.0. Set it explicitly so an SDK upgrade cannot silently change what our
  tokens must carry.

Deployment default: **bind loopback and expose with `tailscale serve`** — tailnet-only, HTTPS,
no `0.0.0.0` listener. `tailscale serve`, never Funnel.

**The Host allow-list is a deployment blocker, not a footnote.** `streamable_http_app()` cannot
know the hostname it will be served behind, so with no explicit `transport_security` it accepts
only `127.0.0.1`, `localhost`, `[::1]` — and `tailscale serve` presents a `.ts.net` host. Every
request then dies with **421 before anything MCP-shaped runs**, the client sees a generic
transport error, and the rejected hostname appears only in the *server's* log. Therefore:

- Set `allowed_hosts` explicitly to the tailscale hostname **in both forms**, `"host.ts.net"`
  and `"host.ts.net:*"`, in **lowercase** — entries are exact strings, Serve forwards the bare
  host without a port, and a case-sensitivity bug (python-sdk#3437) makes an uppercase entry
  reject every WHATWG-`fetch`-based client.
- Treat a `:*` entry as weaker than it looks: python-sdk#3463 accepts `base:9000.evil` because
  the suffix is not validated as a port. Prefer the exact `host` plus `host:443` over a bare
  `host:*`.
- `allowed_origins` matters only to browsers; nothing else sends `Origin`. Keep the 403 test
  anyway, since it is cheap and it is what a browser-based MCP client will hit.
- Run uvicorn with `--proxy-headers` and `--forwarded-allow-ips=<tailscale proxy address>`:
  without it, a redirect such as `/mcp` → `/mcp/` is issued as `http://` and the client refuses
  the downgrade from HTTPS.
- Binding the tailscale interface with `PAPRA_MCP_HOST` does **not** allow-list that hostname;
  it only disarms the localhost default, which leaves *every* Host and Origin accepted. Say what
  you mean in `transport_security`.

No tailnet ACLs are used, by the operator's decision: the tailnet is treated as trusted and
private. That makes the bearer token the sole access control, which is why tokens are
per-device and revocable (see *Operations*), and why Host/Origin validation stays on rather
than being disabled behind the proxy.

`tailscale serve` also injects `Tailscale-User-*` identity headers and strips client-supplied
identity headers, which is a second signal we could require in addition to the token. Not
currently planned: it couples us to one tunnel implementation, and a stolen token from a device
on the tailnet would still pass it.

A static bearer token over a private network is a **normal private/self-hosted MCP deployment**,
not an exotic shortcut: Notion's own MCP server documents it as its production HTTP auth
(`--auth-token`), Outline supports it alongside OAuth, Atlassian supports an API token
alongside OAuth 2.1, and Obsidian's de-facto local REST plugin and Logseq's most-used server
use nothing else. The spec is honest about the status: **authorization is OPTIONAL**, so a
fixed resource-server bearer token is *unaddressed* rather than endorsed — pre-registered OAuth
*client* credentials are the spec-sanctioned thing, and those are not what we are doing.

What it depends on is the client, and the clients differ sharply:

| client | static `Authorization` header | evidence |
|---|---|---|
| pi | yes — `--bearer-token-env-var`, and "OAuth applies to HTTP servers without an `Authorization` header" | installed pi `docs/mcp.md` |
| Claude Code / CLI | yes — `claude mcp add --transport http … --header` | vendor MCP docs |
| Cursor | yes — `headers` in `mcp.json` | vendor MCP docs |
| Claude.ai / Desktop connectors | beta, limited organisations (`static_headers`); otherwise OAuth | vendor connector docs |
| ChatGPT / Codex | OAuth 2.1 with Protected Resource Metadata expected; no static-header option documented | vendor docs |

So the open risk is not "is our scheme legitimate" — it is **which client Tobias actually points
at it**. An OAuth-only client cannot be satisfied by us without building an authorization
server, which is out of scope.

**Discovery document: pick one, deliberately.** With `resource_server_url=<public URL>` we serve
Protected Resource Metadata which advertises an `authorization_servers` issuer that 404s — an
OAuth-capable client follows that pointer into a dead end (python-sdk#3283 documents exactly
this loop with the MCP Inspector). With `resource_server_url=None`, verified in v2.3.0 source:
the bearer gate is still enforced on every request, no `resource_metadata` pointer is sent, and
the fake issuer is advertised nowhere. That is the configuration to ship, with two caveats
written down rather than hidden: it is **not documented as supported** (open PR #3292 proposes a
different fix), and a PRM-dependent client then gets 404s instead of a dead end. Test both
against the intended client and keep whichever works; the choice is version-pinned behaviour,
so it is asserted in a test, not in prose.

**Log and secret handling.** Measured on the same build: uvicorn's access log writes to
**stdout**, and it logs full URLs **including query strings** — the refused query-string token
appeared verbatim in the log. Therefore: access logging disabled or query-stripped, all
logging to stderr, `Authorization` never logged, tokens never accepted from a URL. Verified by
a test that greps its own log output for the configured token.

## Organisation allow-list

The operator curates which Papra organisations MCP may read; the agent may narrow within
that set but never widen it.

- Stored under one `settings` key, `mcp_allowed_org_ids`, holding **Papra** organisation ids.
- `papra-suite mcp-orgs list | add <org-id> | remove <org-id>` manages it until a dashboard
  exists (M4). Each edit is a read-modify-write inside a single `BEGIN IMMEDIATE`
  transaction, so two operators cannot silently lose each other's change.
- Ids are validated **against Papra's organisation list for that API key**, not against our
  local `orgs` table. Our database must not gatekeep what Papra contains: an organisation the
  mail pipeline never touched is still a legitimate one to search. That discovery request runs
  **outside** the write transaction — never hold a SQLite write lock across a network call.
- **Empty allow-list means no access**, not unrestricted access.
- Re-read on **every tool call**: revoking an organisation takes effect on the next call and
  must not need a restart.
- The organisation ceiling (see below) is enforced in `mcp-orgs add` **and** on live reads,
  because the list changes while the server runs. Never silently use a subset.
- Malformed settings fail closed, and MCP opens SQLite **read-only**: no schema
  initialisation, no migrations. A missing database or an unsupported schema is a clear
  startup error, never an empty archive.

## Tools

Four. All read-only; none accepts a path.

| tool | returns |
|---|---|
| `search_documents(query, org_id?, limit?)` | matching documents, metadata only |
| `get_document(document_id, org_id?)` | metadata plus capped content |
| `list_recent_documents(limit)` | newest documents across allowed orgs |
| `list_organizations()` | the allowed orgs only, so the agent can see its own boundary |

- **No snippets.** Measured: `list_documents` items contain no `content` field. Returning a
  snippet would mean fetching every hit individually — a fan-out per search result. Search
  returns metadata; the agent calls `get_document` for what it wants to read.

**Every result carries its `organization_id`.** "Which archive is that from?" is never a
guess, and cross-org results can be attributed after the fact.

### Cross-org merging and fan-out

Papra's endpoints are org-scoped, so an omitted `org_id` means querying the allowed orgs.

- Results merge in a defined order — `createdAt DESC`, then organisation id, then document id
  as tie-breakers. This is date ordering, **not relevance ranking**; we do not have comparable
  scores across orgs.
- `limit` applies **globally**, never per organisation. Correct global newest-N needs each
  org's page to be fetched with `sortField=createdAt&sortOrder=desc` and enough rows to fill
  the merged limit; taking page 1 of each org with `pageSize=limit` is only correct when no org
  contributes more than `limit` results, which holds by construction here and must be asserted
  in a test rather than assumed.
- A `document_id` found in more than one allowed organisation is a **`multiple_matches`**
  error naming the orgs, not whichever fetch finished first. Global uniqueness of Papra document
  ids is plausible and unverified; ambiguity gets reported, not resolved by race.
- For `get_document` without `org_id`, only a genuine **404** permits trying the next
  organisation. A timeout, rate limit, or permission failure aborts the call rather than
  moving on — otherwise an unavailable archive becomes "not in any archive".
- A disallowed `document_id` is never fetched: we only ever ask allowed orgs. This is what
  makes "not fetchable by id" true, rather than merely intended.
- **Partial failure is visible.** Search and recent-listing return `partial: true` plus
  `failed_org_ids`. When **every** allowed org fails there are no results to report and the
  call is an error (`upstream_unavailable`), never an empty list. An unavailable archive must
  never surface as "no documents match", because that is the exact shape of a confident wrong
  answer.
- Ceiling: at most **8 organisations**. Enforced as above. This is an operational bound on
  fan-out, not a security boundary.

### Content size

`get_document` returns at most **20,000 characters** of content, with
`truncated: true` and `chars_omitted: N` alongside it. Measured: `get_document` returns the
full OCR text inline, so an uncapped response is as large as the largest document.

`search_documents` accepts at most **512 characters** of query; `limit` defaults to **10**,
maximum **20**. Exceeding either is a parameter error, not a silent clamp.
- Also bounded, by a returned-field whitelist (`id`, `name`, `organization_id`, `createdAt`,
  `updatedAt`, `mimeType`, `originalSize`, `tags`): `name` 200 chars, org name 100, tag 64, at
  most 10 tags. Anything not on the list is not serialised. On overflow, optional fields
  (`tags`) are dropped first and the result reports `fields_dropped`. Serialized JSON is
  **never** truncated, and ids are never truncated — cutting a JSON document in half to save
  40,000 characters trades a size limit for a parse error, and a truncated id is a document
  that cannot be fetched.
- Upstream responses are capped at **4 MiB enforced while streaming**, not after a `response.json()`
  has already buffered the body. Exceeding it yields `invalid_upstream_response`; a 40 MiB OCR
  dump must not be downloaded in order to be rejected.
- **The omitted tail is unreachable through MCP.** Agreed for v1: Papra's UI is the fallback.
  An agent that does not know this will summarise half a contract and sound certain about it,
  which is why truncation is reported as a flag and a count rather than implied.
- Per-call deadline **15 s**, bounding execution time; the content caps are what bound context
  size. The MCP-to-Papra leg is on the same host, so this is not a network assumption.
  Fan-out runs at most **4 organisations concurrently** and every in-flight upstream call is
  cancelled when the deadline passes — a deadline that bounds duration while orphan requests
  keep running is decoration.

### Untrusted content

Document text is data, never instructions. This is stated once in the server's `instructions`
field at handshake rather than stamped on every result and paid for per call.

It is **guidance, not a boundary**, and the spec says so. A label cannot make an agent obey;
titles, filenames and tags carry injected text as well as bodies; truncation limits payload
size, not intent. A read-only server prevents direct Papra writes through these tools and
nothing else: a document telling the agent to send invoice data somewhere, or to read local
secrets with its *own* tools, is not stopped by anything here.

**Requirement on the consuming agent**, therefore, and not our feature to deliver: document
text must not authorise actions, and sensitive outbound actions need permission controls at
the host. A personal instance with no malicious documents removes neither the injection path
nor the context-window problem — the realistic failure is a 400-page bank statement, not an
exploit.

## Error model

Stable, sanitised codes. Papra response bodies never reach the client or the routine logs.

| code | retryable | meaning |
|---|---|---|
| `not_found_or_not_accessible` | no | genuine 404, or an org outside the allow-list |
| `upstream_unavailable` | yes | timeout, connection failure |
| `upstream_rate_limited` | yes | 429 |
| `upstream_auth` | no | our Papra key is rejected — operator action |
| `invalid_upstream_response` | no | shape we cannot interpret |
| `misconfigured_allowlist` | no | missing or malformed settings |

Collapsing a document 404 with a disallowed org is deliberate: distinguishing them leaks
existence across the allow-list boundary. Collapsing **availability** with **permission**
would be a bug, and is the mistake this table exists to prevent.

## Dependencies

`mcp==2.3.0`, exact pin, matching repo convention. One **direct** dependency; the
transitive tail (uvicorn, pydantic, jsonschema, opentelemetry-api, pyjwt and others) counts
operationally when something breaks.

## Validation

Unit tests, in-process over `httpx.ASGITransport` — no ports, no flakes:

- 401 with no and with wrong credentials; **401 with an existing session id and no
  credentials** (a session id is not authentication); 401 for query-string tokens; 421 and
  403 for Host/Origin.
- Empty allow-list yields no access but starts; a missing or malformed allow-list refuses to
  start; a disallowed `document_id` reads as not found; a forbidden `org_id` filter is rejected;
  org filters only narrow.
- `multiple_matches` when one id resolves in two allowed orgs; global newest-N correctness
  across orgs with unequal document counts.
- Allow-list revocation between two calls; concurrent `mcp-orgs` edits; ceiling violated by a
  live list change; malformed settings fail closed.
- Truncation arithmetic (flag, `chars_omitted`), per-field and aggregate caps, oversized
  upstream responses.
- Fan-out: mixed success and timeout, all orgs failing (error, not empty), `partial` +
  `failed_org_ids`, deterministic ordering, global limit, only-404-continues, cancellation of
  sibling requests at the deadline.
- Log output contains no token, no query string, and nothing on stdout.
- ASGI tests drive the SDK's lifespan correctly, and session-security assertions cover
  **subsequent** requests carrying a session id, not only initialisation.

Integration tests, using the SDK's own Streamable HTTP **client**:

- Full session: initialize → list tools → call tools → GET/DELETE where supported.
- Acceptance criterion 5, end-to-end: real ingest → OCR worker writes content → title worker
  renames → `search_documents` finds the phrase, with a paired control document that must not
  match, a bounded settling window, and an assertion on stored content as well as search.
  Capability check, reported as such — not a regression test for the characterisation
  question, which is still open.
- Read-only access to the app's SQLite with the real WAL sidecar permissions.

Deployment validation:

- Host/Origin behaviour **through the `tailscale serve` path**, not only direct localhost:
  which `Host` and `Origin` values the proxy forwards, whether `X-Forwarded-*` is set, and how
  `PAPRA_MCP_URL` must be written for the SDK's allow-lists to accept it. A server that passes
  localhost tests and rejects every proxied request is a very quiet outage.
- One smoke test with the intended MCP client over Tailscale. Curl-based initialisation is not
  evidence that a real client can use this server.

## Operations

- **Token**: generated, not chosen — `secrets.token_urlsafe(32)`, written down once by the
  operator. The ≥32-character minimum is a floor that rejects `hunter2`, not an entropy
  guarantee.
- **Token rotation**: edit environment, restart, update client. A few seconds of downtime.
  Rotation is deliberately manual; this is one operator.
- **Separate environments, separate credentials**: the pipeline process holds a key that can
  write and a database it may migrate; the MCP process holds a read-only key and opens the
  database read-only. Running both from one environment file defeats both.
- **Revocation**: remove an org with `mcp-orgs remove`; effective on the next call.
- **Health check**: an unauthenticated request returning 401 means *alive and locked*. 200
  there would mean broken.

## Out of scope

Writes of any kind to Papra; file downloads; MCP resources and prompts; OAuth provider or
token issuance; multi-user or per-user authorisation (one token, one operator, one shared
allow-list); relevance ranking; a local search index.

## Open items

1. **Which MCP client — decided.** The targets are **pi** and **Hermes**, the operator's own
   agent stack, both of which send a configured `Authorization` header. OAuth is therefore not
   required and no authorization server will be built. pi's own documentation is explicit: "OAuth
   applies to HTTP servers without an `Authorization` header." Revisit only if a hosted, OAuth-only
   connector (ChatGPT/Codex, Claude.ai connectors) becomes a real requirement — see *Operations*
   for the one concession that keeps that door open.
2. **Search reliability is an open question.** Some probes failed to find content they should
   have; a controlled run did not reproduce the failure. Recorded in
   `2026-11-16-ocr-content-search-intermittent-characterisation-design.md`. Until that closes,
   `search_documents` may return an empty result set for an archive that contains the document,
   which is why empty results are reported with what was searched and which orgs were reached.
3. **Papra `latest` is pinned by digest as of 2026-11-16.** A newer image may change search
   behaviour in either direction; the characterisation spec carries the open question.
4. **Query handling — decided: we tokenise, Papra never sees prose.** Papra ships a search
   *language* (`name:`, `content:`, `tag:`, `date:`, `AND`/`OR`/`NOT`, quoting) and on the pinned
   image it is not even enforced as field syntax (`content:<token>` matched a token that appears
   only in a filename). Measured behaviour, re-measured cleanly on 2026-11-16 with every document
   read back before any search ran (characterisation spec, *Third pass*): **single tokens match by
   prefix on the complete token** (`alpha` finds `alphabet`, `Rechnung` finds `Rechnungen`, `lpha`
   finds nothing), **punctuation separates tokens** (`AZ-99QK` is found by `az` and by `99qk`, and
   a glued `az99qk` finds nothing), and **diacritics are kept, not folded** (`Wäsche` is not found
   by `Waesche`, `Straße` not by `Strasse`). Multi-word queries are not an AND. So:

   - `search_documents(query)` splits the query into words, splits each word again on any
     character that is not a Unicode letter or digit, casefolds, and runs **one Papra query per
     token — at most 6** — intersecting document ids locally before ordering and applying the
     global `limit`. Splitting mirrors what Papra indexes; **gluing fragments back together is
     forbidden**, because the glued form is not in the index. Diacritics are preserved exactly as
     typed (`str.isalnum()`, not `[a-z0-9]`), because folding them is what makes German queries
     unfindable.
   - Quoting is not used: quoting a phrase measured *worse* than the bare form.
   - Field syntax is never forwarded, so a caller cannot express a query our permission model did
     not consider. Organisation scoping stays outside the grammar entirely; a document that tells
     an agent to search `content: <something>` reaches a tokeniser, not Papra.
   - Cost: up to 6 local requests per search, within the 15 s whole-call budget, and it is the
     shape Papra answers reliably. Escalation path if this ever feels clumsy against a real
     archive: Kreuzakt's approach — own the index, stem at index time — accepted as a future
     trade rather than a present one.
   - Consequence for honesty: intersection means a document using a synonym ("Miete" vs "Miete
     Nebenkosten") can be missed for reasons that are ours, not Papra's — and prefix matching means
     a hit can also be broader than the word typed (`Miete` legitimately returns `Mietschulden`).
     The tool description says an empty result is not proof of absence and a non-empty one is not
     proof of an exact match.
   - Consequence for completeness: intersecting one page per token loses documents, so each token
     query is paged until its `documentsCount` is exhausted or a page budget is spent, and a search
     that ran out of budget reports `truncated` rather than looking complete.
