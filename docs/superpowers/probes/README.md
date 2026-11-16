# Measurement probes, 2026-11-16

Throwaway scripts kept because they are the evidence behind three claims in the specs and plans.
**These are not tests.** They run against the scratch stack (`docker-compose.test.yml`, Papra on
`127.0.0.1:12210`), sign up a fresh user and org per run, and leave that org behind — run them
against a scratch instance only. Dependencies: `httpx`, `PyMuPDF`; the MCP ones need `mcp==2.3.0`
and its own HTTP stack (`httpx2`).

The claims they support, and nothing else, are cited elsewhere. Re-run before quoting a number.

## `2026-11-16-content-extraction/` — Papra's own extraction is a second writer of `content`

Cited by the addendum of
`docs/superpowers/specs/2026-11-16-ocr-content-search-intermittent-characterisation-design.md`.
Numbered in execution order; `.log` files are the raw output of the runs quoted there.

| probe | what it establishes |
|---|---|
| `00-concurrent-timeline.py` | a content write and a search race each other from upload time |
| `01-arm-clock.py` | first arm-clock comparison (write at upload vs write after a delay) |
| `02-paired-arms.py` | **the paired 5/5 vs 5/5 result.** Token in content only; filename never contains it |
| `03-positive-control.py` | the positive control: our patched value is replaced by the file's own extracted text |
| `04-window-and-burst.py` | extraction latency on a text PDF; rename-only PATCH leaves content intact; 10-document burst loses 10/10 |
| `05-scan-latency.py` | scan fixture: content visible ~1 s, patch after settle survives 90 s |
| `06-scan-race-and-repair.py` | **the rewrite-is-not-durable result** (`06.log`). Read the content **lengths**: two rows end at 5,639 characters, which is the extraction, after a write that survived to t+20 s |
| `07-delay-sweep.py` | delays of 1/3/5/8/15 s all survived on a 193-byte PDF — the window is usually sub-second on an idle server |

**Known flaws in these scripts**, recorded so their output is read correctly:

- `06-scan-race-and-repair.py` computes its last-line `ours=` flag as `'token' in c2 and token
  in c2` — the first term is a literal substring test and is always false, so that flag is wrong
  on every row. The lengths printed beside it are correct and are what the spec quotes.
- `01-arm-clock.py`'s tick labels are intended times, not measured elapsed times.

## `2026-11-16-search-grammar/` — what Papra's search actually does to a token

Cited by Task 3 of `docs/superpowers/plans/2026-11-16-mcp-search-server.md`.

| probe | what it establishes |
|---|---|
| `papra_prefix.py` | **single tokens match by prefix, not exactly**: `alpha` finds `alphabet`, `Rechnung` finds `Rechnungen`, `Strasse` finds `Strassenverkehr`, `Strassen` does **not** find `Strasse`, `lpha` finds nothing. Every document verified by `GET` before any search, and no filename contains any query token |
| `papra_fold.py` | diacritics: `Wäsche` is found by `Wäsche` and `Wä` but **not** by `Waesche`; `Straße` is not found by `Strasse`; `Grüße` is not found by `Grusse`. **Do not fold or transliterate** |
| `papra_umlaut.py` | punctuation separates tokens (`info@beispiel.de` → `info`, `beispiel`, `de`; `4711/2026`), and short tokens are real (`19`, `Nr`, `Mw` all match) |
| `papra_grammar.py` | `AZ-99QK` is found by `99qk` but a *concatenated* `az99qk` finds nothing — splitting matches Papra's own tokeniser, gluing does not. Also: `searchQuery` paginates correctly and `documentsCount` reports the true match count (5 matches paged at `pageSize=2` → 2,2,1) |

Methodological note: an earlier probe in this family reported the opposite about prefixes,
because it inferred the index state from search results alone. `papra_prefix.py` is the one to
trust: it reads each document back first, so a match can only come from the token under test.

## `2026-11-16-mcp-sdk/` — `mcp==2.3.0` behaviour the MCP plan depends on

| probe | what it establishes |
|---|---|
| `mcp_shapes.py` | asserted-successful session: `dict` return → **no `outputSchema`**, `structured_content=None`, JSON in `content[0].text`; **pydantic-model return → `outputSchema` present and `structured_content` populated**; `str \| None` params become `anyOf[string,null]`; `tools/list` with a session id and **no** `Authorization` → 401; `DELETE` likewise; a token present **only** in `?access_token=` with no header → 401; a notification with a session id → 202 |
| `mcp_validation.py` | the SDK validates argument **types and presence** (`is_error=True`, `Error executing tool search: 1 validation error…`) and nothing else: `limit=0`, `limit=1000000000`, a 10 000-character query, and an unknown extra argument are all accepted and passed through |
| `hosts.py` | with the default transport security, `Host: 127.0.0.1` → **421** while `Host: 127.0.0.1:80` → 200: the default entries are port-wildcards and a bare host never matches |
| `spike-with-known-flaws.py` | kept **because of its flaws**, so they are not repeated: its "session id without credentials" request never actually sent the session id, and its query-string-token request also sent a valid `Authorization` header. Both cases are done correctly in `mcp_shapes.py`. Its value is the working in-process harness (`lifespan_context` + `ASGITransport` + `json_response=True`) |
