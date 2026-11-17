# Papra-as-record / Kreuzakt-as-index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move extraction and search out of Papra and into Kreuzakt, keep Papra as the system of record, and expose the result to agents through one authenticated MCP server that can only see the buckets its token allows.

**Architecture:** Papra holds originals, orgs ("buckets"), tags and names. Kreuzakt (one instance, private network, no published port) extracts text and is the only search authority. Our suite uploads each PDF, correlates the upload to a Kreuzakt queue row, polls to `completed`, writes text and metadata back into Papra under an ownership heuristic, and serves MCP tools that resolve every Kreuzakt id through a local `index_ledger` before returning Papra identities. Kreuzakt has no tenancy concept, so the ledger is the authorization boundary and resolution fails closed.

**Tech Stack:** Python 3.12, `httpx` (async, injectable transport), `sqlite3` via the existing `Store`, `mcp` 2.x (Streamable HTTP), `pytest` + `pytest-asyncio`, Docker Compose, `uv`, `ruff`.

**Spec:** `docs/superpowers/specs/2026-11-17-papra-record-kreuzakt-index-design.md` — read it with this plan (spec §4.2 carries the live Kreuzakt contract, §13/§14 the settled decisions). Half of `2026-11-16-mcp-search-server-design.md` survives: its auth and transport sections are used verbatim; its Papra-searching tools do not.

**Revision:** v2, after advisor review. v1's verification pass could not detect the divergence it existed to repair, its queue correlation guessed at an unmeasured contract, its compose topology contradicted itself, and it used Kreuzakt's synthetic filename as an ownership baseline. Those are corrected in Tasks 2–11; the review's findings are summarised at the end.

## Global Constraints

- Papra stays the system of record. Nothing here deletes or moves an original from Papra.
- Kreuzakt is reachable only from the private compose network it shares with the suite container. **No published host port in production**; no tailnet name; its UI never served.
- Every client-visible URL in an MCP response points at Papra. No Kreuzakt path (`/api/documents/…`, `/documents/…`, `/original`, `/thumbnail`) or Kreuzakt integer id may leave the proxy as an identifier.
- Every MCP handler resolves through `index_ledger`, filters on the token's buckets, checks the document's **live Papra state**, and **fails closed** on lookup failure: unknown, stale, wrong-generation, deleted or unresolvable Kreuzakt ids yield nothing.
- MCP tools are read-only in v1. No upload path exists — Kreuzakt cannot know which bucket a file belongs to.
- **PDFs only**, unchanged `_is_pdf` gate.
- Hard error when the index is down. No Papra-backed search fallback: "found nothing" keeps meaning "not in your archive".
- Papra's `content` column is a convenience copy that **converges eventually**. Convergence is measured by comparing **Papra's current content against Kreuzakt's current content** — never by comparing Papra against what we last delivered, which cannot see Papra overwriting us.
- Empty extracted text is never written over non-empty Papra content. `completed` means the pipeline finished, not that the page is blank.
- Ownership of `name`, `documentDate`, `notes` is established against **Papra-side baselines stored in the ledger**, never against Kreuzakt's `original_filename` (which is our synthetic upload name). First write of `documentDate`/`notes` happens only where Papra's value is empty; a non-empty value with no baseline is protected on sight.
- Never overwrite a value we did not write; a human edit permanently protects that field for that reference. Clearing protection is an explicit operator command with its own ledger write.
- A manual edit to Papra's `content` **will** be overwritten by the next delivery or verification pass. This is policy, stated, not a defect.
- Deleting one Papra reference must not delete a shared Kreuzakt document while another active reference remains. Failed upstream deletes are retried from durable ledger state, not from memory.
- Rebuilds are handled by **generation + re-index from Papra's bytes**, never by matching ids or filename prefixes. A restored volume whose generation is not current serves nothing.
- `DOCUMENTS_OCR_LANGUAGES=deu,eng` is set by the operator in Papra's `.env` and mirrored in the test compose. It **replaces** the default language list; each traineddata is fetched on first use, so a recreated container needs outbound internet once per language.
- Auto-tagging config (`AI_IS_ENABLED`, `AUTO_TAGGING_ENABLED`, per-org `aiAutoTaggingEnabled`, `AI_ADAPTERS`) stays the operator's UI concern. No code requests `organizations:update`.
- Minted API keys keep `documents:read documents:update documents:delete organizations:read` and nothing wider.
- Kreuzakt fields are snake_case; `/api/search` caps `limit` at 100 and returns **no score**; hits carry full `content` which must not be forwarded.
- Upload filenames are bridge-chosen and unique: `papra-<documentId>-<hash8>.pdf`.
- Migration numbering: `ingest-visibility` owns 002, so `index_ledger` is **003**.
- Images pinned by digest: Papra `sha256:d1f596a35332ad04d8a69d88a5e19bc82acd47f2bc2fe687dd2145b1ef5a8b19`, GreenMail `sha256:309ba544ea482e5d4265d09921cdab07c8dad2cc4481d715c9b8ff5d3f0a149f`, Kreuzakt `sha256:4a0109d03d663338fd967662aa0151ec7684d2ba31cdfd12f1bf4f3901b1cd8d`.
- **Integration tests run on atlas**, the `x86_64` server, as a process on that host (`ssh atlas` then `make test-integration`). `DOCKER_HOST=ssh://atlas` moves Docker operations only — it does not move pytest's network namespace, so a laptop test process cannot reach containers that publish no port. The laptop runs unit tests with fake transports.
- Gates per task: `make lint`, `make test-unit`, plus `PAPRA_REQUIRE_STACK=1 make test-integration` on atlas wherever a task touches the stack.
- Mutation checks are **executed**, not imagined: apply the patch with `sed`, run the named test, confirm it fails, then `git checkout --` the file. The command and the failing test name go in the task's commit message.
- `Makefile` keeps `VENV ?= .venv/bin/`; destructive targets stay separate; the daily loop never `down -v`s the scratch stack.
- TDD: failing test first, see it fail, minimal implementation, see it pass, commit.

## File Structure

| file | responsibility |
| --- | --- |
| `papra_suite/kreuzakt_client.py` (new) | the only code that speaks Kreuzakt HTTP; parses snake_case into dataclasses; drops every Kreuzakt-side link; owns queue correlation |
| `papra_suite/indexing.py` (new) | the bridge: `index` job handler, `deliver_content`, `deliver_metadata`, `verify_once`, `reconcile_deletions`, `reconcile_generation` |
| `papra_suite/store.py` (modify) | migration 003 `index_ledger` + the whole ledger API including generation and cleanup state |
| `papra_suite/pipeline.py` (modify) | `enqueue_index_job`, verification and cleanup loops in `serve` |
| `papra_suite/config.py` (modify) | Kreuzakt URL, delivery flags, scan caps, bounds |
| `papra_suite/mcp_tokens.py`, `mcp_server.py`, `mcp_tools.py` (new) | tokens with per-token buckets; MCP app (auth/transport from the 2026-11-16 spec); the six tools |
| `papra_suite/webhooks.py` (dropped, see Task 10) | Papra event receiver — out of v1; the six-hour verification pass is the mechanism instead |
| `papra_suite/metadata_cli.py`, `jobs_cli.py`, `backfill_cli.py`, `ledger_cli.py` (new) | operator surface |
| `papra_suite/ocr.py`, `titles.py` (deleted in Task 16) | the workers Kreuzakt replaces |
| `docker-compose.test.yml`, `Makefile`, `README.md` (modify) | pinned Kreuzakt, private network, runbooks |
| `docs/superpowers/probes/2026-11-17-kreuzakt-contracts/` (new) | Task 3 probe scripts + findings |

---

## Shared test scaffolding

Named once here so every task's tests use the same names and no implementer invents a second version.
All live in `tests/conftest.py` unless a task says otherwise.

| helper | signature and behaviour |
| --- | --- |
| `H` | module constant, `"ab" * 32`, the standing file hash |
| `store` | fixture: `Store(tmp_path / "db.sqlite")` with `migrate()` run, migrated to head |
| `make_job` | `make_job(*, kind="index", papra_org_id="org-a", document_id="doc-1", content_hash=..., payload=None) -> Job` — builds a `Job` without touching the store |
| `seed_job` | `seed_job(store, *, kind, status, papra_org_id="org-a", document_id="doc-1", attempts=0) -> str` — inserts a job row directly |
| `failing_index_job` | `failing_index_job(store, *, attempts=3) -> str` — an `index` job already marked failed with `error` set |
| `seed_ledger` | `seed_ledger(store, *, org_id="org-a", document_id="doc-1", file_hash=H, papra_original_name="scan-0001.pdf", kreuzakt_id=None, generation=1, written_name=None) -> None` |
| `seed_reference` | `seed_reference(store, *, deleted=False, **kw)` — `seed_ledger` plus `deleted_at` when asked |
| `seed_two_bucket_reference` | two rows in `org-a`/`org-b` sharing one `kreuzakt_id` |
| `seed_listing` | `seed_listing(fake_papra, *, count, mime="application/pdf", org_id="org-a") -> list[str]` — ids `doc-0…doc-N`, returns them |
| `seed_deleted_in_papra` | `seed_deleted_in_papra(fake_papra, *, count, org_id="org-a") -> None` |
| `FakePapra` | `get_document` returns a dict copy from `.documents`; `update_document` records into `.last_patch` and bumps `.patch_calls`; `list_documents` honours `page_index`/`page_size` and raises when `.raise_on_list`; `.raise_on_get` makes `get_document` raise `PapraError`; `.delete_calls`/`.get_calls` counted |
| `FakeKreuzakt` | `.high_water` int, `.upload_calls`/`.high_water_calls`/`.calls` lists, `.document` (a `KzDocument`), `.hits`, `.raw_search`, `.never_completes`, `.missing_documents`, `.error`, `.health`, `.on_wait` (callable fired inside `wait_for_queue_row`), `fail_delete(id)` |
| fixtures `fake_papra` / `fake_kz` | the injected instances of `FakePapra` / `FakeKreuzakt` used by every test above; `caplog` is pytest's own fixture and needs no declaration here |
| `_fill` / `papra_doc` | `_fill(n)` returns `n` placeholder rows so a page can be made "full" deliberately; `papra_doc(**over)` builds a Papra detail dict with `isDeleted=False` |
| `make_client` / `queue_page` / `load` | Task 5's MockTransport rig; `queue_page(rows)` wraps rows in the envelope captured in Task 3; `load(name)` reads `tests/fixtures/<name>` |
| `kz_doc` | `kz_doc(*, title="Rechnung 4711", description="Rechnung ueber 1.234,56 Euro", content=KZ_TEXT, document_date="2025-10-01") -> KzDocument` |
| `KZ_TEXT` / `sha_of` / `seed_delivered` | the canonical Kreuzakt text, `sha_of(text)`, and a ledger row already marked delivered for it |
| `ref_with` | Task 8's `LedgerRef` builder with the ownership columns defaulted |
| `ctx` / `ctx_all` / `ctx_scoped_to` | `TokenContext` instances: one bucket, all buckets, or a named bucket |
| `scopes` | `scopes() -> list[tuple[str, str]]` of `(label, org_id)`, four buckets |
| `hit` / `hit_for` | `hit(kreuzakt_id) -> KzHit`, `hit_for(papra_document_id) -> KzHit` bound to a seeded reference |
| `stats_of` | `stats_of(page) -> ResolveStats`, i.e. `page.stats` |
| `describe` | `describe(page) -> str` — the human summary line MCP returns alongside the page |
| `event_body` / `signed` / `sign_headers` / `post_event` | archived with Task 10; nothing builds them, so no later task may reference them |
| `call_tool` | `call_tool(client, *, token, name, **args) -> pydantic model` — one MCP tool call over the SDK client |

---

### Task 1: Land the two predecessor branches

**Files:** merge-only; `papra_suite/store.py`, `Makefile`, `docker-compose.test.yml`, `papra_suite/titles.py`.

**Interfaces:**
- Consumes: `ingest-visibility`@1327625, `ocr-prompt-language`@d389d12, both off `simplify-ingest`@e86a134.
- Produces: `Store` at `user_version = 2` with `ingest_events`, `Store.record_ingest_event`, `Store.dedup_exists/add_dedup`; Makefile targets `lint test-unit test-integration stack-up stack-down stack-digests`; digests pinned in compose; language-neutral title prompt. Every later task assumes this state.

- [ ] **Step 1: Confirm branch heads before merging anything**

```bash
git fetch --all --prune
git log --oneline -1 ingest-visibility && git log --oneline -1 ocr-prompt-language
git status --short
```
Expected `1327625` and `d389d12`, working tree clean. If either differs, stop: merging an unknown state into the branch that carries the archive's pipeline is not a shortcut.

- [ ] **Step 2: Merge in dependency order**

```bash
git checkout simplify-ingest
git merge --no-ff ingest-visibility
git merge --no-ff ocr-prompt-language
```
Only expected conflict is `papra_suite/titles.py`/`tests/test_titles.py`: keep ingest-visibility's structure, take ocr-prompt-language's prompt text.

- [ ] **Step 3: Unit gates**

Run `make lint && make test-unit`. Expected: clean lint, 445+ passing, including the migration-002 test that upgrades a copy of a real v1 database and asserts `user_version` advances.

- [ ] **Step 4: Stack gates on atlas**

Run `ssh atlas 'cd ~/papra-ai-assistant && git pull --ff-only && make stack-up && PAPRA_REQUIRE_STACK=1 make test-integration'`. Expected green; paste the `make stack-digests` output into the commit message so the image set is recorded.

- [ ] **Step 5: Record and commit**

```bash
git add docs/superpowers/HANDOFF.md
git commit -m "docs(handoff): merge ingest-visibility and ocr-prompt-language into simplify-ingest"
```

---

### Task 2: Put Kreuzakt in the test stack, with a topology that does not contradict itself

v1 of this plan attached Kreuzakt to a network its own comment said it must not be on, used `internal: true` (which would have blocked the omlx egress it needs), and claimed `DOCKER_HOST=ssh://atlas` was enough for pytest. It is not: the test process must run on the host that runs the containers.

**Files:** Modify `docker-compose.test.yml`, `Makefile`, `README.md`.

**Interfaces:**
- Consumes: nothing.
- Produces: compose service `kreuzakt` on network `indexnet` (non-internal, **no `ports:` section**), reachable from the suite at `http://kreuzakt:3000`; env `PAPRA_KREUZAKT_URL`; a **test-only** loopback publication `127.0.0.1:12400` used solely because the test client runs as a host process; documented deviation between test and production exposure.

- [ ] **Step 1: Add the service**

```yaml
  # Kreuzakt is the extractor and the only search authority. It has NO
  # authentication, so PRODUCTION publishes no port: the suite container is its
  # only client, over a network nothing else joins.
  #
  # The one deliberate deviation in the test stack: integration tests run as a
  # process on the test host, so the test compose publishes 127.0.0.1:12400 to
  # give that process a path in. Everything about Kreuzakt's *behaviour* under
  # test is the production behaviour; only this listener differs, and the
  # production snippet below has no ports: key at all.
  #
  # Not `internal: true` — Kreuzakt must reach omlx on the Mac Studio over the
  # tailnet, and an internal network has no route out. Isolation here comes from
  # publishing no port and joining no other network, not from a flag.
  # Digest as of 2026-11-17. Upstream CI builds linux/amd64 only, deliberately;
  # atlas is x86_64. Refresh like papra below, then re-run the suite.
  kreuzakt:
    image: ghcr.io/anaisbetts/kreuzakt@sha256:4a0109d03d663338fd967662aa0151ec7684d2ba31cdfd12f1bf4f3901b1cd8d
    environment:
      DATA_DIR: /data
      OCR_VLM_BASE_URL: "${OMLX_BASE_URL:-}"
      OCR_VLM_API_KEY: "${OMLX_API_KEY:-}"
      OCR_VLM_MODEL: "${OMLX_OCR_MODEL:-}"
      OPENAI_BASE_URL: "${OMLX_BASE_URL:-}"
      OPENAI_API_KEY: "${OMLX_API_KEY:-}"
      METADATA_LLM_MODEL: "${OMLX_TEXT_MODEL:-}"
    volumes:
      - kreuzakt-data:/data
    ports:
      - "127.0.0.1:12400:3000"          # TEST ONLY; absent in production
    networks: [indexnet]
    healthcheck:
      test: ["CMD-SHELL", "bash -c 'exec 3<>/dev/tcp/127.0.0.1/3000'"]
      interval: 3s
      timeout: 2s
      retries: 40
volumes:
  kreuzakt-data:
networks:
  indexnet:
```
Keep Papra and GreenMail exactly as they are (their published `127.0.0.1:` ports stay), and keep `DOCUMENTS_OCR_LANGUAGES: "deu,eng"` on Papra so the tested topology matches the deployed one.

- [ ] **Step 2: Prove the two properties that matter, and paste the evidence**

```bash
make stack-up
docker inspect kreuzakt --format '{{json .NetworkSettings.Networks}}' | python3 -m json.tool
```
Expected: exactly one network, `*_test-indexnet`. Then prove Papra cannot be reached from Kreuzakt:
```bash
docker compose -f docker-compose.test.yml exec kreuzakt \
  bash -c 'curl -s -m 3 http://papra:1221/api/health || echo PASS-unreachable'
```
Expected `PASS-unreachable` (different networks, no DNS). Write both observed outputs into the compose comment; a claim nobody saw is a claim that rots.

- [ ] **Step 3: Make the digest target cover it, and state where tests run**

```make
stack-digests:
	docker images --digests --format '{{.Repository}} {{.Digest}}' | grep -E 'papra|greenmail|kreuzakt'
```
In `README.md`: the production snippet (same service block **without** `ports:`), `PAPRA_KREUZAKT_URL=http://kreuzakt:3000` for the suite in its Dokploy stack, the reachability list ("the suite container, and nothing else"), and one unambiguous line: *integration tests run on atlas as a process on that host — `ssh atlas && PAPRA_REQUIRE_STACK=1 make test-integration` — because `DOCKER_HOST=ssh://atlas` does not move the test process's network namespace.*

- [ ] **Step 4: Commit**

```bash
git add docker-compose.test.yml Makefile README.md
git commit -m "feat(compose): pinned Kreuzakt on a private network; correct test topology claims"
```

---

### Task 3: Measure the two contracts the bridge depends on (gate for Tasks 5, 6, 10)

Two contracts are unmeasured and load-bearing: Kreuzakt's queue semantics under retry, and Papra's webhook delivery. Everything downstream guesses at them, so they get measured first, in one probe task, with scripts kept in the repo.

**Files:** Create `docs/superpowers/probes/2026-11-17-kreuzakt-contracts/{probe_queue.py,probe_webhooks.py,listener.py,README.md}`; Modify the spec §10.

**Interfaces:**
- Consumes: the Task 2 stack; a real API key from the stack's own signup + GreenMail verification.
- Produces a written verdict per question. No product code.

- [ ] **Step 1: Kreuzakt queue and upload contract — nine questions**

`probe_queue.py`, run against `http://127.0.0.1:12400`, answering each with captured JSON in `README.md`:
1. Exact response of `POST /api/upload` for one file and for two files in one request.
2. Duplicate upload of identical bytes **while the first is still processing**: rows, statuses, eventual `document_id`s.
3. Duplicate upload **after completion**: new row? new id?
4. `/api/queue` response shape: envelope key, ordering (ascending or descending id), and **the `limit`/`offset` or `page` parameters it accepts** — walk until empty and report total rows for a known number of uploads.
5. Does `/api/queue` have a maximum `limit`, and does it ever hide old rows (retention)?
6. Is `filename` the upload name, the stored name, or both, and is matching by exact equality sufficient?
7. Behaviour of `POST /api/queue/{id}/retry`: new row or reused row?
8. What `rescan`/`reindex-all` does to `document_id`, `updated_at`, and `content`.
9. Whether `document_id` can ever be null on a `completed` row.
10. **Whether `id` is monotonic and the default ordering is by id** — the high-water-mark correlation
    is only sound if both hold, and the client refuses to correlate on a scan that did not prove
    exhaustion. If ordering turns out to be by `created_at` with ties, correlation must switch to
    `(created_at, id)` and this plan changes before Task 5 is written.

Record the answer to #9 explicitly: Task 6 treats null-on-completed as a retryable anomaly and that decision needs evidence.

- [ ] **Step 2: Kreuzakt queue test fixture derived from the measurement**

Paste the two captured payloads into `tests/fixtures/kreuzakt_queue_page.json` and `tests/fixtures/kreuzakt_document.json` (the document fixture from the 2026-11-17 probe already exists in `docs/superpowers/probes/2026-11-17-kreuzakt/`). Tasks 5, 6, 13 build fakes from these files, so a guessed field name cannot survive the suite.

- [ ] **Step 3: Papra webhook delivery — five questions**

`probe_webhooks.py` + `listener.py` (writes `{path, headers, body}` JSONL, and answers 500 on the third delivery to observe retry):
1. Is `POST /api/organizations/{org}/webhooks` body `{"url", "events"}`? If it 400s, paste the validation message verbatim and use the field names it names.
2. Payload: where the event name lives, what the document object contains, and **whether `document.updated` includes a `changes` list**.
3. **Attribution: does the payload identify who made the change** (a user id, an API key id, or an actor field)? This decides whether Task 10 can distinguish our PATCH from a human rename at all.
4. Signature: which header, and does `hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()` reproduce it? If not, capture the exact signed string layout.
5. Retry: on a 500, how many attempts, at what intervals, and what `webhookUrlAllowedHostnames` does to an in-network URL like `http://suite:8080/hook` versus an external one.

- [ ] **Step 4: Write the verdicts into the spec, both ways**

Replace the §10 bullets with the measured answers. If queue correlation cannot be made unambiguous (#4, #6), Task 5's correlation stays as written but Task 6 gains "refuse to deliver when correlation is ambiguous" as a hard rule instead of a preference. Attribution (#3, step 3) no longer decides any task in this plan — Task 10 was dropped by the operator on 2026-11-17 — but record the answer anyway, because it is the one fact that decides whether a future receiver could distinguish our own PATCH from a human rename.

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/probes/2026-11-17-kreuzakt-contracts tests/fixtures docs/superpowers/specs
git commit -m "docs(probe): measure Kreuzakt queue/upload semantics and Papra webhook delivery"
```

---

### Task 4: `index_ledger` migration 003 and the ledger API

The ledger is the authorization boundary, so it is built and tested before anything writes to it. It carries generation, cleanup state, and the Papra-side ownership baselines — three things v1 got wrong.

**Files:** Modify `papra_suite/store.py`; Create `tests/test_store_ledger.py`; Modify `tests/test_store_migrations.py`.

**Interfaces:**
- Consumes: `Store` at `user_version = 2`.
- Produces:
  - `LedgerRef(papra_org_id, papra_document_id, kreuzakt_id, file_hash, papra_original_name, delivered_at, delivered_sha, delivered_generation, written_name, name_protected, written_date, date_protected, written_notes, notes_protected, repair_count, last_repair_at, deleted_at, cleanup_state, cleanup_attempts, cleanup_next_at)` — all strings/ints/bools, `None` where nullable.
  - `Store.ledger_upsert_reference(*, papra_org_id, papra_document_id, file_hash, papra_original_name) -> None` (upsert must not clobber existing protection or delivery state)
  - `Store.ledger_set_kreuzakt_id(*, papra_org_id, papra_document_id, kreuzakt_id, generation) -> None`
  - `Store.ledger_get(papra_org_id, papra_document_id) -> LedgerRef | None`
  - `Store.ledger_resolve(kreuzakt_ids, *, generation) -> list[LedgerRef]` — active **and current-generation** references only
  - `Store.ledger_needs_index(*, generation, org_id=None, limit=100) -> list[LedgerRef]` — references with no id, wrong generation, or `deleted_at` cleared
  - `Store.ledger_mark_deleted(*, papra_org_id, papra_document_id) -> tuple[int | None, int]` — `(kreuzakt_id, remaining_active_same_generation)`; `(-1, -1)` if no id
  - `Store.ledger_mark_restored(*, papra_org_id, papra_document_id) -> None`
  - `Store.ledger_record_delivery(*, papra_org_id, papra_document_id, delivered_at, delivered_sha, generation) -> None`
  - `Store.ledger_record_metadata(*, papra_org_id, papra_document_id, name=None, date=None, notes=None) -> None`
  - `Store.ledger_protect(*, papra_org_id, papra_document_id, field) -> None` / `Store.ledger_clear_protection(*, field=None, org_id=None, document_id=None) -> int`
  - `Store.ledger_bump_repair(*, papra_org_id, papra_document_id) -> int` — rolling 24 h window, resets to 1 when `last_repair_at` is older
  - `Store.ledger_stats(*, org_ids=None) -> dict[str, int]` — keys `references, indexed_current, delivered, extraction_empty, protected_name, protected_date, protected_notes, repairs_24h, cleanup_pending`
  - `Store.get_generation() -> int` / `Store.bump_generation() -> int` (via `settings["index_generation"]`, default `1`)
  - `Store.ledger_queue_cleanup(*, papra_org_id, papra_document_id, due_at, kreuzakt_id, generation) -> None`, `Store.ledger_cleanup_due(*, now, limit=50) -> list[LedgerRef]`, `Store.ledger_cleanup_result(*, papra_org_id, papra_document_id, ok, next_at=None, reason=None) -> None`

- [ ] **Step 1: Write the failing migration test**

```python
def test_migration_003_adds_ledger_with_generation_and_cleanup(tmp_path):
    build_store_at_version(tmp_path / "db.sqlite", 2)
    store = Store(tmp_path / "db.sqlite"); store.migrate()
    assert store._conn.execute("PRAGMA user_version").fetchone()[0] == 3
    cols = {r["name"] for r in store._conn.execute("PRAGMA table_info(index_ledger)")}
    assert {"papra_original_name", "delivered_generation", "cleanup_state",
            "cleanup_kreuzakt_id", "cleanup_generation",
            "cleanup_attempts", "cleanup_next_at", "written_date", "written_notes",
            "date_protected", "notes_protected"} <= cols
    idx = {r["name"] for r in store._conn.execute("PRAGMA index_list(index_ledger)")}
    assert {"idx_index_ledger_kreuzakt", "idx_index_ledger_hash",
            "idx_index_ledger_cleanup"} <= idx
```
Run `pytest tests/test_store_migrations.py -k 003 -v` → FAIL on `user_version`.

- [ ] **Step 2: Write the failing model tests**

```python
H = "ab" * 32

def two_refs(store):
    for org, doc in (("org-a", "doc-1"), ("org-b", "doc-2")):
        store.ledger_upsert_reference(papra_org_id=org, papra_document_id=doc,
                                      file_hash=H, papra_original_name=f"{doc}.pdf")
        store.ledger_set_kreuzakt_id(papra_org_id=org, papra_document_id=doc,
                                     kreuzakt_id=7, generation=store.get_generation())
    return store

def test_two_references_share_one_kreuzakt_id(store):        # no unique index
    assert len(two_refs(store).ledger_resolve([7], generation=1)) == 2

def test_delete_is_reference_counted(store):
    store = two_refs(store)
    assert store.ledger_mark_deleted(papra_org_id="org-a", papra_document_id="doc-1") == (7, 1)
    assert [r.papra_document_id for r in store.ledger_resolve([7], generation=1)] == ["doc-2"]
    assert store.ledger_mark_deleted(papra_org_id="org-b", papra_document_id="doc-2") == (7, 0)
    assert store.ledger_resolve([7], generation=1) == []

def test_stale_generation_resolves_to_nothing(store):        # the rebuild rule
    store = two_refs(store); store.bump_generation()
    assert store.ledger_resolve([7], generation=store.get_generation()) == []
    assert [r.papra_document_id for r in store.ledger_needs_index(generation=2)] == ["doc-1", "doc-2"]

def test_upsert_does_not_clobber_protection(store):
    store = two_refs(store)
    store.ledger_protect(papra_org_id="org-a", papra_document_id="doc-1", field="name")
    store.ledger_upsert_reference(papra_org_id="org-a", papra_document_id="doc-1",
                                  file_hash=H, papra_original_name="scan.pdf")
    assert store.ledger_get("org-a", "doc-1").name_protected is True
    assert store.ledger_get("org-a", "doc-1").kreuzakt_id == 7

def test_clear_protection_counts_and_scopes(store):
    store = two_refs(store)
    for field in ("name", "date", "notes"):
        store.ledger_protect(papra_org_id="org-a", papra_document_id="doc-1", field=field)
    assert store.ledger_clear_protection(field="name", org_id="org-a") == 1
    ref = store.ledger_get("org-a", "doc-1")
    assert not ref.name_protected and ref.date_protected and ref.notes_protected

def test_repair_window_rolls_over(store):
    assert store.ledger_bump_repair(papra_org_id="o", papra_document_id="d") == 1
    assert store.ledger_bump_repair(papra_org_id="o", papra_document_id="d") == 2
    store._conn.execute("UPDATE index_ledger SET last_repair_at = '2000-01-01T00:00:00+00:00'")
    store._conn.commit()
    assert store.ledger_bump_repair(papra_org_id="o", papra_document_id="d") == 1

def test_cleanup_due_ignores_done_and_future_rows(store):
    for doc in ("due", "done", "future"):
        store.ledger_upsert_reference(papra_org_id="o", papra_document_id=doc,
                                      file_hash=H, papra_original_name=f"{doc}.pdf")
    store.ledger_queue_cleanup(papra_org_id="o", papra_document_id="due",
                              due_at="2026-11-17T00:00:00+00:00",
                              kreuzakt_id=7, generation=store.get_generation())
    def repatch(doc, state, nxt):
        store._conn.execute("UPDATE index_ledger SET cleanup_state=?, cleanup_next_at=? "
                            "WHERE papra_document_id=?", (state, nxt, doc))
    store._conn.commit()
    repatch("done", "done", "2026-11-17T00:00:00+00:00")
    repatch("future", "pending", "2099-01-01T00:00:00+00:00")
    store._conn.commit()
    due = store.ledger_cleanup_due(now="2026-11-17T12:00:00+00:00")
    assert [r.papra_document_id for r in due] == ["due"]
```
Run `pytest tests/test_store_ledger.py -v` → FAIL on missing methods.

- [ ] **Step 3: Implement migration 003**

```python
def _migration_003_index_ledger(conn: sqlite3.Connection) -> None:
    """One row per PAPRA document: a reference, not a pairing.

    Kreuzakt dedupes by file hash, so identical bytes in two buckets produce one
    Kreuzakt document and two rows sharing kreuzakt_id. No unique index on
    kreuzakt_id: resolution returns a set and deletes are reference-counted.

    kreuzakt ids are only meaningful within an index generation. A wiped or
    restored Kreuzakt restarts its ids at 1, so bumping the generation makes
    every stale row unresolvable and eligible for re-index from Papra's bytes —
    which is why nothing here matches by id or filename prefix.
    """
    conn.executescript("""
        CREATE TABLE index_ledger (
            papra_org_id        TEXT NOT NULL,
            papra_document_id   TEXT NOT NULL,
            kreuzakt_id         INTEGER,
            file_hash           TEXT NOT NULL,
            papra_original_name TEXT,
            delivered_at        TEXT,
            delivered_sha       TEXT,
            delivered_generation INTEGER,
            written_name        TEXT,
            name_protected      INTEGER NOT NULL DEFAULT 0,
            written_date        TEXT,
            date_protected      INTEGER NOT NULL DEFAULT 0,
            written_notes       TEXT,
            notes_protected     INTEGER NOT NULL DEFAULT 0,
            repair_count        INTEGER NOT NULL DEFAULT 0,
            last_repair_at      TEXT,
            deleted_at          TEXT,
            cleanup_state       TEXT NOT NULL DEFAULT 'none',
            cleanup_kreuzakt_id INTEGER,
            cleanup_generation  INTEGER,
            cleanup_attempts    INTEGER NOT NULL DEFAULT 0,
            cleanup_next_at     TEXT,
            PRIMARY KEY (papra_org_id, papra_document_id)
        );
        CREATE INDEX idx_index_ledger_kreuzakt ON index_ledger (kreuzakt_id)
            WHERE kreuzakt_id IS NOT NULL AND deleted_at IS NULL;
        CREATE INDEX idx_index_ledger_hash ON index_ledger (file_hash);
        CREATE INDEX idx_index_ledger_cleanup ON index_ledger (cleanup_state, cleanup_next_at)
            WHERE cleanup_state = 'pending';
    """)
```
`MIGRATIONS = [_migration_001_initial, _migration_002_ingest_events, _migration_003_index_ledger]`. `ledger_mark_deleted` sets `deleted_at`, then counts remaining active rows of the **current generation** in the same `with self._lock, self._conn:` block, so a concurrent delete cannot both see "one left" and both skip the upstream delete.

- [ ] **Step 4: Run the tests, then execute the mutation check**

Run `pytest tests/test_store_ledger.py tests/test_store_migrations.py -v` → PASS.
```bash
sed -i.bak 's/AND deleted_at IS NULL//' papra_suite/store.py
pytest tests/test_store_ledger.py::test_delete_is_reference_counted -x -q   # must FAIL
rm -f papra_suite/store.py.bak && git checkout -- papra_suite/store.py
```
Put that command pair and the failing test name in the commit message.

- [ ] **Step 5: Commit**

```bash
git add papra_suite/store.py tests/test_store_ledger.py tests/test_store_migrations.py
git commit -m "feat(store): index_ledger with generation, ownership baselines, cleanup state"
```

---

### Task 5: `KreuzaktClient` with correlation the probe proves

**Files:** Create `papra_suite/kreuzakt_client.py`, `tests/test_kreuzakt_client.py`; Modify `papra_suite/config.py`, `tests/test_config.py`.

**Interfaces:**
- Consumes: the payloads captured in Task 3.
- Produces:
  - `KreuzaktError(message: str, status: int | None = None)`
  - `QueueEntry(id: int, filename: str, status: str, error: str | None, document_id: int | None, page_count: int | None)`
  - `KzDocument(id, title, description, content, document_date, original_filename, updated_at)` — **no link fields exist on this type**
  - `KzHit(id, title, document_date, added_at, mime_type, original_filename)` — no `content`, no `score`
  - `KreuzaktClient(base_url, *, timeout_s=30.0, transport=None)` with `aclose()`, `health()`, `upload(filename, content) -> str`, `queue_high_water_mark() -> int`, `wait_for_queue_row(filename, *, after_id, poll_s, timeout_s) -> QueueEntry`, `queue_page(*, page, limit) -> list[QueueEntry]`, `get_document(int) -> KzDocument`, `search(query, *, page=1, limit=100, offset=0) -> tuple[list[KzHit], int]`, `list_documents(*, page=1, limit=100, since=None) -> list[dict]`, `delete_document(int) -> None`, `retry_queue(int) -> None`, `queue_counts() -> dict[str, int]`
  - `Config.kreuzakt_base_url`, `Config.index_content_delivery`, `Config.index_metadata_delivery`, `Config.index_scan_pages`, `Config.index_max_scan_rows`, `Config.index_poll_interval_s`, `Config.index_wait_timeout_s`, `Config.index_query_max_chars`, `Config.mcp_url`, `Config.mcp_port`

- [ ] **Step 1: Write the failing tests against the captured fixtures**

```python
def load(name):  # tests/fixtures written by Task 3 — no hand-guessed payloads
    return json.loads((Path(__file__).parent / "fixtures" / name).read_text())

def make_client(responses):
    """responses: list of (path_fragment, json_body) served in call order."""
    calls = []
    def handler(request):
        calls.append((request.method, str(request.url)))
        for fragment, body in responses:
            if fragment in str(request.url):
                return httpx.Response(200, json=body)
        return httpx.Response(404, json={"error": "unhandled"})
    return KreuzaktClient("http://kreuzakt:3000",
                          transport=httpx.MockTransport(handler)), calls

async def test_upload_returns_relative_path_and_sends_files_field():
    client, calls = make_client([("/api/upload", load("kreuzakt_upload_response.json"))])
    assert await client.upload("papra-d1-a1b2.pdf", b"%PDF") == "papra-d1-a1b2.pdf"
    assert calls[0][1].endswith("/api/upload")
    assert b'name="files"' in upload_bodies[0]        # the handler records request bodies

async def test_high_water_mark_walks_pages_and_returns_max_id():
    client, _ = make_client([("/api/queue?page=1", queue_page([{"id": 101}, ..._fill(99)],
                                                              full=True)),
                             ("/api/queue?page=2", queue_page([{"id": 150}])),
                             ])
    assert await client.queue_high_water_mark() == 150


async def test_high_water_mark_refuses_to_return_an_unproven_scan():
    """Every page full to the cap means we never proved exhaustion: fail closed."""
    client, _ = make_client([("/api/queue", queue_page([{"id": i} for i in range(100)],
                                                      full=True))])
    with pytest.raises(KreuzaktError, match="scan incomplete"):
        await client.queue_high_water_mark()

async def test_wait_for_queue_row_requires_exact_filename_and_greater_id():
    """Suffix matching and 'newest row' are both guesses; neither is used."""
    entry = await client.wait_for_queue_row("papra-d1-a1b2.pdf", after_id=101,
                                           poll_s=0, timeout_s=1)
    assert entry.document_id == 7

async def test_wait_for_queue_row_ignores_a_stale_row_below_the_mark():
    client, _ = make_client([("/api/queue", queue_page([
        {"id": 50, "filename": "papra-d1-a1b2.pdf", "status": "completed",
         "document_id": 3, "error": None},                       # an earlier attempt
    ]))])
    with pytest.raises(KreuzaktError, match="no queue row"):
        await client.wait_for_queue_row("papra-d1-a1b2.pdf", after_id=101,
                                       poll_s=0, timeout_s=0.05)


async def test_wait_for_queue_row_times_out_loudly_not_as_none():
    client, _ = make_client([("/api/queue", queue_page([]))])
    with pytest.raises(KreuzaktError, match="no queue row"):
        await client.wait_for_queue_row("papra-d1-a1b2.pdf", after_id=1,
                                       poll_s=0, timeout_s=0.05)


async def test_two_terminal_rows_for_one_upload_are_ambiguous_not_resolved():
    client, _ = make_client([("/api/queue", queue_page([
        {"id": 102, "filename": "papra-d1-a1b2.pdf", "status": "completed",
         "document_id": 7, "error": None},
        {"id": 103, "filename": "papra-d1-a1b2.pdf", "status": "failed",
         "document_id": 9, "error": "boom"},
    ]))])
    with pytest.raises(KreuzaktError, match="ambiguous"):
        await client.wait_for_queue_row("papra-d1-a1b2.pdf", after_id=101,
                                       poll_s=0, timeout_s=5)


async def test_completed_row_with_null_document_id_is_an_error_not_a_silent_none():
    """Task 3 question 9 decides whether this can happen; the client refuses anyway."""
    client, _ = make_client([("/api/queue", queue_page([
        {"id": 102, "filename": "papra-d1-a1b2.pdf", "status": "completed",
         "document_id": None, "error": None},
    ]))])
    with pytest.raises(KreuzaktError, match="without document_id"):
        await client.wait_for_queue_row("papra-d1-a1b2.pdf", after_id=101,
                                       poll_s=0, timeout_s=5)


async def test_search_clamps_limit_to_100_and_never_exposes_content_or_score():
    client, calls = make_client([("/api/search", load("kreuzakt_search_page.json"))])
    hits, total = await client.search("rechnung", limit=500)
    assert "limit=100" in calls[-1][1]
    assert total == 1 and len(hits) == 1
    assert {f for f in vars(hits[0])} == {"id", "title", "document_date", "added_at",
                                          "mime_type", "original_filename"}
async def test_get_document_has_no_field_holding_a_kreuzakt_path():
    client, _ = make_client([("/api/documents/2", load("kreuzakt_document.json"))])
    doc = await client.get_document(2)
    assert not any("/documents/" in str(v) for v in vars(doc).values())
async def test_non_2xx_raises_with_status_and_truncated_text():
    """PapraError truncates response text to 200 chars; copy that discipline."""
```
Every assertion above is written out; where a test is listed with a one-line body, that body **is** the test, not a sketch of one.

- [ ] **Step 2: Run to verify failure**

Run `pytest tests/test_kreuzakt_client.py -v` → `ModuleNotFoundError`.

- [ ] **Step 3: Implement the correlation precisely**

```python
_SEARCH_LIMIT_MAX = 100
_QUEUE_PAGE_SIZE = 100
_QUEUE_PAGES_MAX = 20          # 2000 rows of history is plenty for 500 documents


class KreuzaktClient:
    async def upload(self, filename: str, content: bytes) -> str:
        response = await self._request(
            "POST", "/api/upload",
            files={"files": (filename, content, "application/pdf")})
        payload = response.json()
        if payload.get("failedCount") or not payload.get("files"):
            raise KreuzaktError(f"upload rejected: {payload.get('errors')!r}")
        return payload["files"][0]["relativePath"]

    async def queue_page(self, *, page: int, limit: int = _QUEUE_PAGE_SIZE) -> list[QueueEntry]:
        body = (await self._request("GET", "/api/queue",
                                    params={"page": page, "limit": limit})).json()
        rows = body.get("entries") if "entries" in body else body.get("items", [])
        return [_to_queue_entry(row) for row in rows]

    async def queue_high_water_mark(self) -> int:
        """Highest queue id right now, or a loud failure. Recorded BEFORE upload,
        then our row is the one whose filename matches exactly and whose id
        exceeds this. Newest-row guessing breaks the moment two uploads are in
        flight, and a *partially scanned* mark is worse than none: it would let a
        pre-existing row be mistaken for ours. So exhaustion is proven (a short
        page) or the scan raises; the page cap is a fuse, not an exit.
        """
        highest = 0
        for page in range(1, _QUEUE_PAGES_MAX + 1):
            rows = await self.queue_page(page=page)
            if not rows:
                return highest
            highest = max([highest] + [row.id for row in rows])
            if len(rows) < _QUEUE_PAGE_SIZE:
                return highest
        raise KreuzaktError(
            f"queue scan incomplete after {_QUEUE_PAGES_MAX * _QUEUE_PAGE_SIZE} rows; "
            "refusing to correlate on a partial high-water mark")
```
`wait_for_queue_row` loops: walk pages, collect rows with `row.filename == filename and row.id > after_id`, raise `KreuzaktError("ambiguous correlation: N rows")` if more than one is terminal, return the first `completed`/`failed`, and raise `KreuzaktError("no queue row for {filename} within {timeout_s}s")` on timeout. `delete_document` accepts 204 and 404 (already gone is gone); anything else raises.

- [ ] **Step 4: Config fields, all env-parsed**

```python
kreuzakt_base_url: str          # PAPRA_KREUZAKT_URL, default "http://kreuzakt:3000"
index_content_delivery: bool    # PAPRA_INDEX_CONTENT_DELIVERY, default False
index_metadata_delivery: bool   # PAPRA_INDEX_METADATA_DELIVERY, default False
legacy_workers: bool            # PAPRA_LEGACY_WORKERS, default True; when False the ocr/title worker
                                # loops never start and no legacy job is enqueued
index_scan_pages: int           # PAPRA_INDEX_SCAN_PAGES, default 5
index_max_scan_rows: int        # PAPRA_INDEX_MAX_SCAN_ROWS, default 500
index_poll_interval_s: float    # PAPRA_INDEX_POLL_INTERVAL_S, default 2.0
index_wait_timeout_s: int       # PAPRA_INDEX_WAIT_TIMEOUT_S, default 900
index_query_max_chars: int      # PAPRA_INDEX_QUERY_MAX_CHARS, default 512
mcp_url: str                    # PAPRA_MCP_URL  (Host/Origin allow-list, from the 11-16 spec)
mcp_port: int                   # PAPRA_MCP_PORT, default 8765
```
`kreuzakt_base_url` and `mcp_url` go through `_require_base_url`, so a typo fails at `papra-suite check` rather than at first upload. Both delivery flags default False: the bridge lands write-nothing.

- [ ] **Step 5: Gates, then commit**

Run `make lint && make test-unit` → PASS.
```bash
git add papra_suite/kreuzakt_client.py papra_suite/config.py tests/test_kreuzakt_client.py tests/test_config.py
git commit -m "feat(index): Kreuzakt client with high-water-mark correlation"
```

---

### Task 6: The `index` job kind — upload, correlate, record; write nothing to Papra

**Files:** Create `papra_suite/indexing.py`, `tests/test_index_job.py`; Modify `papra_suite/pipeline.py`, `tests/test_pipeline.py`; Create `tests/integration/test_index_pipeline.py`.

**Interfaces:**
- Consumes: `Store.enqueue_job/claim_next_job/mark_job_failed/recover_running_jobs`, `Store.get_generation`, `ledger_upsert_reference`, `ledger_set_kreuzakt_id`, `ledger_needs_index`, `KreuzaktClient`.
- Produces:
  - `index_upload_name(job: Job) -> str`
  - `async def run_index_job(kz, store, job, *, papra, poll_s, wait_timeout_s) -> IndexOutcome(kreuzakt_id: int, content_len: int, empty_content: bool, delivered: bool)`
  - `async def download_original(papra, job: Job) -> bytes` — Papra is the only source of bytes, always, including after a rebuild
  - `IndexFailed(Exception)`, `IndexTimeout(Exception)`
  - `enqueue_index_job(store, *, papra_org_id, document_id, file_hash, name, mime, ingested_via) -> str`
- Eligibility rule that replaces v1's mistake: a document needs indexing when the ledger has **no current-generation id**, which is derived from the ledger, never from `document_seen` — `document_seen` records ingestion history, so a rebuild would have looked already-done.

- [ ] **Step 1: Write the failing tests**

```python
def test_upload_name_binds_document_and_hash():
    assert index_upload_name(make_job(document_id="doc-1", content_hash="a1b2c3d4e5")) \
        == "papra-doc-1-a1b2c3d4.pdf"

async def test_index_writes_nothing_to_papra_while_content_delivery_is_off(store, fake_papra):
    """The last phase where a bridge bug cannot corrupt the record."""
    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                  poll_s=0, wait_timeout_s=5)
    assert fake_papra.patched == []
    ref = store.ledger_get(job.papra_org_id, job.document_id)
    assert ref.kreuzakt_id == 7 and ref.delivered_generation == store.get_generation()
    assert ref.papra_original_name == "scan-0001.pdf"

async def test_stale_job_hash_is_abandoned_not_delivered(store, fake_papra):
    """Source changed while the job waited: our bytes are not the record's bytes."""
    fake_papra.document["originalSha256Hash"] = "different"
    with pytest.raises(IndexFailed, match="source hash changed"):
        await run_index_job(fake_kz, store, job, papra=fake_papra, poll_s=0, wait_timeout_s=5)
    assert store.ledger_get(job.papra_org_id, job.document_id).kreuzakt_id is None

async def test_existing_current_id_skips_upload_entirely(store, fake_kz, fake_papra):
    seed_ledger(store, kreuzakt_id=7, generation=1)
    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                 poll_s=0, wait_timeout_s=5)
    assert outcome.kreuzakt_id == 7
    assert fake_kz.upload_calls == [] and fake_kz.high_water_calls == []


async def test_wrong_generation_reuploads_and_rebinds_id(store, fake_kz, fake_papra):
    seed_ledger(store, kreuzakt_id=7, generation=1)
    store.bump_generation()                       # Kreuzakt was rebuilt under us
    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                 poll_s=0, wait_timeout_s=5)
    assert outcome.kreuzakt_id == fake_kz.high_water + 1
    ref = store.ledger_get(job.papra_org_id, job.document_id)
    assert ref.delivered_generation == store.get_generation()


async def test_empty_content_is_recorded_as_extraction_empty(store, fake_papra, fake_kz):
    fake_kz.document = KzDocument(id=7, title="", description="", content="   \n",
                                 document_date=None, original_filename="f", updated_at=None)
    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra,
                                 poll_s=0, wait_timeout_s=5)
    assert outcome.empty_content is True and outcome.delivered is False
    assert store.ledger_get(job.papra_org_id, job.document_id).kreuzakt_id == 7
    assert any("extraction-empty" in r.message for r in caplog.records)


async def test_generation_change_mid_flight_discards_the_result(store, fake_papra, fake_kz):
    """The race generation exists to catch: rebuild lands between upload and bind."""
    fake_kz.on_wait = lambda: store.bump_generation()
    with pytest.raises(IndexFailed, match="generation changed mid-flight"):
        await run_index_job(fake_kz, store, job, papra=fake_papra, poll_s=0, wait_timeout_s=5)
    assert store.ledger_get(job.papra_org_id, job.document_id).kreuzakt_id is None


async def test_queue_timeout_raises_so_the_job_retries_with_backoff(store, fake_papra, fake_kz):
    fake_kz.never_completes = True
    with pytest.raises(KreuzaktError, match="no queue row"):
        await run_index_job(fake_kz, store, job, papra=fake_papra, poll_s=0,
                           wait_timeout_s=0.05)
    store.mark_job_failed(job.id, "no queue row within 0.05s")   # what the worker does
    claimed = store.get_job(job.id)
    assert claimed.status == "pending" and claimed.attempts == 1  # retriable, not dead
    assert claimed.next_attempt_at > claimed.updated_at
async def test_ambiguous_correlation_is_reported_not_guessed(store, fake_kz):
    with pytest.raises(KreuzaktError, match="ambiguous"):
        await run_index_job(fake_kz, store, job, papra=fake_papra, poll_s=0, wait_timeout_s=5)
    assert store.ledger_get(job.papra_org_id, job.document_id).kreuzakt_id is None
```

- [ ] **Step 2: Implement the handler**

```python
async def run_index_job(kz, store, job, *, papra, poll_s=2.0, wait_timeout_s=900):
    generation = store.get_generation()          # fenced: see the note below
    ref = store.ledger_get(job.papra_org_id, job.document_id) or _create(store, job)

    document = await papra.get_document(job.papra_org_id, job.document_id)
    if document.get("isDeleted"):
        raise IndexFailed("document deleted in papra")           # deletion pass owns it
    if (document.get("originalSha256Hash") or job.content_hash) != job.content_hash:
        raise IndexFailed("source hash changed; job abandoned")

    if ref.kreuzakt_id is not None and ref.delivered_generation == generation:
        kreuzakt_id = ref.kreuzakt_id                            # nothing to upload
    else:
        filename = index_upload_name(job)
        after_id = await kz.queue_high_water_mark()              # before upload, always
        await kz.upload(filename, await download_original(papra, job))
        entry = await kz.wait_for_queue_row(filename, after_id=after_id,
                                           poll_s=poll_s, timeout_s=wait_timeout_s)
        if entry.status == "failed":
            raise IndexFailed(f"kreuzakt queue error: {entry.error}")
        if entry.document_id is None:
            raise IndexFailed("queue row completed without document_id")
        kreuzakt_id = entry.document_id
        # Conditional accept: a rebuild that happened while we waited has to win.
        # Binding an id from the old index under the new generation would point a
        # live reference at a meaningless integer, which is the exact failure
        # generation was added to prevent.
        bound = store.ledger_set_kreuzakt_id(papra_org_id=job.papra_org_id,
                                             papra_document_id=job.document_id,
                                             kreuzakt_id=kreuzakt_id,
                                             generation=generation,
                                             only_if_generation=generation)
        if not bound:
            raise IndexFailed("generation changed mid-flight; result discarded")

    kz_doc = await kz.get_document(kreuzakt_id)
    empty = not kz_doc.content.strip()
    if empty:
        logger.info("extraction-empty papra=%s kreuzakt=%s", job.document_id, kreuzakt_id)
    if store.settings_flag("index_content_delivery"):            # written in Task 7
        await deliver_content(papra, store, ref, kz_doc, generation=generation)
    return IndexOutcome(kreuzakt_id=kreuzakt_id, content_len=len(kz_doc.content),
                        empty_content=empty, delivered=not empty)
```
`ledger_set_kreuzakt_id` therefore becomes `-> bool`, performing `UPDATE … WHERE delivered_generation IS NULL OR delivered_generation = :expected` inside one transaction, and `Store.get_generation()` is read at job start only.

The delivery call is also gated **inside** `deliver_content` (Task 7), not only here: a flag that is checked solely by the scheduler can be bypassed by an inbound trigger, the convergence pass, or a CLI invocation. (Task 10's webhook path was dropped, but the gate stays: it is what makes any future inbound path safe by construction rather than by review.)
`_create` records `papra_original_name=document["name"]` at first sight — the baseline Task 8 needs, captured here because this is the first moment we hold the Papra document.

- [ ] **Step 3: Enqueue and wire, preserving crash ordering**

```python
def enqueue_index_job(store, *, papra_org_id, document_id, file_hash, name, mime, ingested_via):
    """Enqueue index work for one ingested document.

    Enqueue FIRST, record seen SECOND, unchanged from enqueue_ocr_job: a crash
    between the writes leaves the document unseen, so the sweep re-enqueues it
    (enqueue is idempotent on kind+org+doc+hash) and records it. Seen-first would
    strand it forever, because the sweep skips documents it has already seen.
    """
    if mime != "application/pdf":
        return ""
    job_id = store.enqueue_job(kind="index", papra_org_id=papra_org_id,
                               document_id=document_id, content_hash=file_hash,
                               payload={"file_hash": file_hash, "name": name})
    if not store.settings_flag("index_content_delivery"):
        store.enqueue_job(kind="ocr", papra_org_id=papra_org_id, document_id=document_id,
                          content_hash=file_hash,
                          payload={"file_hash": file_hash, "name": name})
    store.record_document_seen(papra_org_id=papra_org_id, papra_document_id=document_id,
                               name=name, content_hash=file_hash)
    return job_id
```
`serve()` gains `worker_loop(store, kind="index", handler=...)` beside `ocr`/`title`, and `settings_flag` is a one-line `Store` helper reading `settings` (set from `Config` at startup, so the CLI and the daemon agree). During this phase the old writers keep running: Papra keeps the text and titles it has today.

- [ ] **Step 4: Integration test on atlas**

```python
@pytest.mark.skipif(not os.environ.get("PAPRA_REQUIRE_STACK"), reason="needs the compose stack")
async def test_mail_attachment_is_indexed_without_touching_papra_content(stack):
    before = (await stack.papra.get_document(stack.org_id, doc_id))["content"]
    await wait_for_ledger(stack.store, stack.org_id, doc_id, timeout_s=120)
    after = (await stack.papra.get_document(stack.org_id, doc_id))["content"]
    ref = stack.store.ledger_get(stack.org_id, doc_id)
    assert after == before                                   # Papra untouched
    assert ref.kreuzakt_id is not None
    assert (await stack.kreuzakt.get_document(ref.kreuzakt_id)).content   # structure, not wording
```
Run on atlas: `ssh atlas 'cd ~/papra-ai-assistant && PAPRA_REQUIRE_STACK=1 make test-integration'` → PASS.

- [ ] **Step 5: Commit**

```bash
git add papra_suite/indexing.py papra_suite/pipeline.py papra_suite/store.py tests/test_index_job.py tests/integration/test_index_pipeline.py
git commit -m "feat(index): index job kind with generation-bound correlation"
```

---

### Task 7: Content delivery, the blank-page rule, and a convergence pass that can see divergence

v1's `deliver_content` returned early when Kreuzakt's hash equalled `delivered_sha` — which is exactly the state Papra is in after it overwrites a document we delivered. The pass could not detect the one failure it existed to fix. Verification now compares Papra's current content to Kreuzakt's current content.

**Files:** Modify `papra_suite/indexing.py`, `papra_suite/pipeline.py`, `papra_suite/papra_client.py`; Create `tests/test_content_delivery.py`; Modify `tests/integration/test_index_pipeline.py`.

**Interfaces:**
- Consumes: `PapraClient.get_document/update_document`, `KreuzaktClient.get_document`, `Store.ledger_record_delivery/ledger_bump_repair/ledger_stats/get_generation`.
- Produces:
  - `async def deliver_content(papra, store, ref, kz_doc, *, generation) -> str` → `"delivered" | "skipped-empty" | "skipped-current"`
  - `VerifyResult(scanned: int, diverged: int, repaired: int, suppressed: int, empty: int, lookup_errors: int)`
  - `async def verify_once(store, papra, kz) -> VerifyResult`
  - `async def _verification_loop(store, papra, kz, *, interval_s=21600, stop=None)` in `pipeline.py`

- [ ] **Step 1: Write the failing tests — the first one is the mutation target**

```python
async def test_delivery_gate_blocks_every_writer_while_the_flag_is_off(fake_papra, fake_kz, store):
    """One gate, four paths. A scheduler-only flag can be bypassed by an incident path."""
    store.set_setting("index_content_delivery", "false")
    assert (await run_index_job(fake_kz, store, job, papra=fake_papra,
                                poll_s=0, wait_timeout_s=5)).delivered is False
    for coro in (deliver_content(fake_papra, store, ref, fake_kz.document, generation=1),
                 deliver_metadata(fake_papra, store, ref, fake_kz.document)):
        with pytest.raises(DeliveryDisabled):
            await coro
    assert (await verify_once(store, fake_papra, fake_kz)).repaired == 0
    assert fake_papra.patch_calls == 0


async def test_papra_overwriting_us_is_detected_and_repaired(fake_papra, fake_kz, store):
    """The defect v1 shipped: delivered_sha still matches, Papra's text is not ours."""
    seed_delivered(store, sha_of=KZ_TEXT)                  # ledger says delivered
    fake_papra.document["content"] = "Papra wrote this after us"
    result = await verify_once(store, fake_papra, fake_kz)
    assert result.diverged == 1 and result.repaired == 1
    assert fake_papra.document["content"] == KZ_TEXT

async def test_converged_document_is_left_alone(fake_papra, fake_kz, store):
    fake_papra.document["content"] = KZ_TEXT
    result = await verify_once(store, fake_papra, fake_kz)
    assert result.diverged == 0 and fake_papra.patch_calls == 0

async def test_empty_kreuzakt_text_never_overwrites_non_empty_papra(fake_papra, store):
    """Mutation check (b): remove the guard and this fails."""
    fake_papra.document["content"] = "Vorhandener Text"
    assert await deliver_content(fake_papra, store, ref, kz_doc_with(content=""),
                                generation=1) == "skipped-empty"
    assert fake_papra.patch_calls == 0

async def test_repair_suppressed_after_two_divergences_in_24h(fake_papra, fake_kz, store, caplog):
    fake_papra.document["content"] = "Papra wrote this after us"
    for _ in range(3):
        result = await verify_once(store, fake_papra, fake_kz)
    assert result.suppressed == 1 and fake_papra.patch_calls == 2
    assert any("diverged more than twice in 24h" in r.message for r in caplog.records)
    assert "/reprocess" not in caplog.text      # names the observable, never a culprit

async def test_lookup_error_fails_closed_and_is_counted(fake_papra, fake_kz, store):
    fake_papra.raise_on_get = True
    result = await verify_once(store, fake_papra, fake_kz)
    assert result.lookup_errors == 1 and result.repaired == 0

async def test_wrong_generation_row_is_not_repaired(store, fake_papra, fake_kz):
    store.bump_generation()
    assert (await verify_once(store, fake_papra, fake_kz)).scanned == 0
```

- [ ] **Step 2: Implement, with the guard ordered before every shortcut**

```python
async def deliver_content(papra, store, ref, kz_doc, *, generation) -> str:
    """Write Kreuzakt's text into Papra, unless doing so would lose text.

    The flag is checked here as well as in the scheduler: convergence, the webhook
    receiver and the CLI all reach this function, and a gate that lives only in the
    loop can be bypassed by the paths that matter most during an incident.
    Order also matters within the body: the emptiness guard runs before any
    skip-shortcut, because a guard placed after a cheap equality check can be
    reordered into a wipe by someone optimising this function later.
    """
    if not store.settings_flag("index_content_delivery"):
        raise DeliveryDisabled("content delivery is off")     # intake checks first; others get it loud
    current = await papra.get_document(ref.papra_org_id, ref.papra_document_id)
    papra_content = current.get("content") or ""
    sha = hashlib.sha256(kz_doc.content.encode()).hexdigest()
    if not kz_doc.content.strip():
        logger.info("extraction-empty, not writing papra=%s", ref.papra_document_id)
        return "skipped-empty"
    if sha == hashlib.sha256(papra_content.encode()).hexdigest():
        if ref.delivered_sha != sha:                       # someone else wrote our text
            store.ledger_record_delivery(papra_org_id=ref.papra_org_id,
                                         papra_document_id=ref.papra_document_id,
                                         delivered_at=kz_doc.updated_at or "",
                                         delivered_sha=sha, generation=generation)
        return "skipped-current"
    await papra.update_document(ref.papra_org_id, ref.papra_document_id, content=kz_doc.content)
    store.ledger_record_delivery(papra_org_id=ref.papra_org_id,
                                 papra_document_id=ref.papra_document_id,
                                 delivered_at=kz_doc.updated_at or "",
                                 delivered_sha=sha, generation=generation)
    return "delivered"
```
`verify_once` iterates `ledger_resolve`-eligible rows **of the current generation** in pages of 100 (a new `Store.ledger_delivered_refs(*, generation, after, limit)` cursor helper, added in this task), fetches Papra's detail and Kreuzakt's document, and calls `deliver_content` only when the two differ. It bumps and reads the repair counter **before** repairing: `count = store.ledger_bump_repair(...)`; `if count > 2: suppressed += 1; logger.error("content diverged more than twice in 24h, leaving alone papra=%s", ...)` and no write.

- [ ] **Step 3: Papra client gains the remaining fields, with the notes cap**

```python
async def update_document(self, org_id: str, document_id: str, *, name: str | None = None,
                          content: str | None = None, document_date: str | None = None,
                          notes: str | None = None) -> dict:
    body = {k: v for k, v in {"name": name, "content": content,
                              "documentDate": document_date, "notes": notes}.items()
            if v is not None}
```
Callers pass notes already truncated by `truncate_notes(text, limit=2000)` from `indexing.py`: cut at the last newline below the limit, append `…`, never split a word or exceed Papra's 2048 cap and get the whole PATCH rejected.

- [ ] **Step 4: Loop, `check` line, and the drain step before flipping delivery on**

`serve()` starts `_verification_loop` alongside `_sweep_loop`. `check` prints `index: 512 refs, 508 current, 505 delivered, 3 extraction-empty, 2 repairs(24h), 1 cleanup-pending`.

Handover is a gate, not a sequence of hopes, and it is ordered so that no two writers can own the same
column at the same moment. A momentarily empty queue is **not** proof of quiescence — a worker loop can
claim the next job a second later — so the producers are stopped first:

1. `PAPRA_LEGACY_WORKERS=false` in the suite env, recreate the container. This stops the `ocr` and
   `title` **worker loops from claiming anything** and stops `enqueue_index_job` creating new legacy
   jobs. `Config.legacy_workers` (default `true`) is that flag; with it false `serve()` does not start
   those loops at all.
2. Prove quiescence rather than assuming it, twice a minute apart:
   ```bash
   papra-suite jobs --kind ocr --status pending    # empty
   papra-suite jobs --kind ocr --status running    # empty
   papra-suite jobs --kind ocr --drain             # refuses while anything exists
   ```
3. Only then `PAPRA_INDEX_CONTENT_DELIVERY=true`, recreate the container.

The same three steps apply to metadata with `--kind title` and `PAPRA_INDEX_METADATA_DELIVERY`. Content
and metadata handover stay independent and independently reversible until Task 16 removes the code.
`Store.count_jobs(kind, status)` and the `--drain` refusal make step 2 enforced by code rather than by
good intentions, and `test_drain_refuses_while_any_job_is_pending_or_running` proves it.

- [ ] **Step 5: Integration, mutation check, commit**

Integration: overwrite Papra's content behind our back, run `verify_once`, assert Papra's content **equals the text captured from Kreuzakt for that document** — transport fidelity is exact even when the model is not.
```bash
sed -i.bak '/if not kz_doc.content.strip():/,+2d' papra_suite/indexing.py
pytest tests/test_content_delivery.py::test_empty_kreuzakt_text_never_overwrites_non_empty_papra -x -q
git checkout -- papra_suite/indexing.py && rm -f papra_suite/indexing.py.bak
git add -A && git commit -m "feat(index): content delivery and divergence-visible convergence"
```

---

### Task 8: Metadata delivery — name, date, notes — on Papra-side baselines

**Files:** Modify `papra_suite/indexing.py`; Create `papra_suite/metadata_cli.py`, `tests/test_metadata_delivery.py`; Modify `papra_suite/__main__.py`, `papra_suite/pipeline.py`.

**Interfaces:**
- Consumes: `ledger_get/ledger_protect/ledger_clear_protection/ledger_record_metadata`, `PapraClient.get_document/update_document`, `truncate_notes`.
- Produces:
  - `normalize_date(value: str | None) -> str | None` — ISO `YYYY-MM-DD` or `None`; empty and unparseable model output become `None` and are skipped
  - `OwnershipVerdict(owned: bool, reason: str)` and `def ownership(ref, *, field, current) -> OwnershipVerdict`
  - `async def deliver_metadata(papra, store, ref, kz_doc) -> MetadataResult(name_action, date_action, notes_action)`, each `"written" | "protected" | "unchanged" | "skipped"`
  - CLI `papra-suite metadata (--restore-auto | --force) [--org-id ID] [--document-id ID] [--field name|date|notes] [--dry-run]`

- [ ] **Step 1: Write the failing tests, including the baseline rules the review demanded**

```python
def ref_with(**over):
    base = dict(papra_org_id="org-a", papra_document_id="doc-1", file_hash=H,
                papra_original_name="scan-0001.pdf", written_name=None, name_protected=False,
                written_date=None, date_protected=False, written_notes=None,
                notes_protected=False)
    return LedgerRef(**{**base, **over})

async def test_rename_allowed_when_papra_holds_our_last_name(fake_papra, store):
    fake_papra.document["name"] = "Altes Auto-Title.pdf"
    ref = ref_with(written_name="Altes Auto-Title.pdf")
    assert (await deliver_metadata(fake_papra, store, ref, kz_doc())).name_action == "written"
    assert store.ledger_get("org-a", "doc-1").written_name == "Rechnung 4711"
async def test_first_rename_allowed_when_papra_still_holds_its_own_original_name(fake_papra, store):
    """Baseline is Papra's name at first sight — NOT Kreuzakt's original_filename,
    which is our synthetic papra-<id>-<hash>.pdf and proves nothing."""
    fake_papra.document["name"] = "scan-0001.pdf"
    assert (await deliver_metadata(fake_papra, store, ref_with(), kz_doc())).name_action == "written"

async def test_human_rename_protects_permanently_and_logs_once(fake_papra, store, caplog):
    fake_papra.document["name"] = "Meine Steuer 2025"
    for _ in range(3):
        assert (await deliver_metadata(fake_papra, store, ref_with(), kz_doc())).name_action == "protected"
    assert fake_papra.patch_calls == 0
    assert sum("protecting name" in r.message for r in caplog.records) == 1

async def test_synthetic_name_in_papra_is_never_treated_as_ours(fake_papra, store):
    fake_papra.document["name"] = "papra-doc-1-a1b2c3d4.pdf"
    assert (await deliver_metadata(fake_papra, store, ref_with(), kz_doc())).name_action == "protected"

async def test_date_first_write_only_when_papra_date_is_empty(fake_papra, store):
    fake_papra.document["documentDate"] = "2024-05-05T00:00:00.000Z"   # Papra inferred it
    assert (await deliver_metadata(fake_papra, store, ref_with(), kz_doc())).date_action == "protected"
    fake_papra.document["documentDate"] = None
    assert (await deliver_metadata(fake_papra, store, ref_with(), kz_doc())).date_action == "written"

async def test_blank_model_date_is_skipped_not_protected(fake_papra, store):
    assert (await deliver_metadata(fake_papra, store, ref_with(),
                                  kz_doc(document_date=""))).date_action == "skipped"

async def test_notes_recorded_as_truncated_value_we_actually_wrote(fake_papra, store):
    long = ("wort " * 900).strip()
    await deliver_metadata(fake_papra, store, ref_with(), kz_doc(description=long))
    written = store.ledger_get("org-a", "doc-1").written_notes
    assert written == fake_papra.last_patch["notes"] and len(written) <= 2001

async def test_dates_are_normalised_before_comparison(fake_papra, store):
    fake_papra.document["documentDate"] = "2025-10-01"
    assert (await deliver_metadata(fake_papra, store,
            ref_with(written_date="2025-10-01"), kz_doc(document_date="2025-10-01"))).date_action == "unchanged"
```

- [ ] **Step 2: Implement ownership as one readable function**

```python
def ownership(ref, *, field: str, current) -> OwnershipVerdict:
    """Is this field ours to write? A heuristic, not a compare-and-swap.

    Papra offers no conditional write, so a rename landing between our read and
    our write is overwritten; the worst case is one lost rename, never lost text.
    For date and notes the first write requires an empty Papra value, because a
    non-empty value we have no baseline for might be a human's.
    """
    if getattr(ref, f"{field}_protected"):
        return OwnershipVerdict(False, "protected")
    ours = getattr(ref, f"written_{field}", None)
    if ours is not None:
        return OwnershipVerdict((current or "") == ours, "not-ours")
    if field == "name":
        return OwnershipVerdict((current or "") == (ref.papra_original_name or ""), "no-baseline")
    return OwnershipVerdict(not (current or ""), "empty-required")
```
`deliver_metadata` starts with the same boundary gate as content — `if not store.settings_flag("index_metadata_delivery"): raise DeliveryDisabled("metadata delivery is off")` — so a webhook trigger or a manual call cannot rename documents while the old title worker still owns `name`. It then fetches Papra's document once, evaluates all three fields, writes in one PATCH where anything changed, records **exactly the values sent** via `ledger_record_metadata`, and calls `ledger_protect` for each field whose verdict was `not-ours`, logging once per field.

- [ ] **Step 3: The CLI, which is the only way protection is undone**

`metadata --restore-auto` calls `ledger_clear_protection` for the selected scope and prints the count; `metadata --force` sets an in-process `ignore_protection` flag for one run, writes regardless, prints how many human values it overwrote, and requires `--yes-i-mean-it` to act (without it, it prints the plan and exits 1). Both support `--dry-run`. No delivery path ever clears protection on its own.

- [ ] **Step 4: Stop feeding title jobs, drained**

When `index_metadata_delivery` is on, `enqueue_index_job` skips the `ocr` enqueue entirely (no OCR text means no title input, and the `title` chain is dead by construction), and `README.md` requires `papra-suite jobs --kind title --drain` before the flag is flipped. Handover of content and of metadata are separate, each reversible until Task 16 removes the code.

- [ ] **Step 5: Mutation check (c), gates, commit**

```bash
sed -i.bak 's/if getattr(ref, f"{field}_protected"):/if False:/' papra_suite/indexing.py
pytest tests/test_metadata_delivery.py::test_human_rename_protects_permanently_and_logs_once -x -q
git checkout -- papra_suite/indexing.py && rm -f papra_suite/indexing.py.bak
make lint && make test-unit
git add -A && git commit -m "feat(index): metadata delivery on Papra-side baselines with explicit un-protect"
```

---

### Task 9: Delete and restore propagation, reference-counted and durable

**Files:** Modify `papra_suite/indexing.py`, `papra_suite/pipeline.py`, `papra_suite/store.py` (if a query is missing); Create `tests/test_deletion_propagation.py`.

**Interfaces:**
- Consumes: `ledger_mark_deleted/mark_restored/ledger_queue_cleanup/ledger_cleanup_due/ledger_cleanup_result/ledger_resolve`, `KreuzaktClient.delete_document`, `PapraClient.list_documents`, `Store.get_generation`.
- Produces:
  - `async def reconcile_deletions(store, papra, kz) -> DeletionResult(refs_marked: int, upstream_deleted: int, upstream_pending: int, restored: int, aborted: bool, capped: int)`
  - `async def run_cleanup(store, kz) -> CleanupResult(attempted: int, deleted: int, deferred: int, failed: int)`
  - `MAX_DELETIONS_PER_PASS = 20`, `CLEANUP_MAX_ATTEMPTS = 5`, `CLEANUP_BACKOFF_S = 300`

- [ ] **Step 1: Write the failing tests**

```python
async def test_shared_document_survives_one_reference_being_deleted(fake_papra, fake_kz, store):
    """Probe-confirmed: two buckets, one Kreuzakt document."""
    await reconcile_deletions(store, fake_papra, fake_kz)
    assert fake_kz.deleted == []
    assert [r.papra_document_id for r in store.ledger_resolve([7], generation=1)] == ["doc-2"]

async def test_last_reference_deletion_is_queued_then_executed(fake_papra, fake_kz, store):
    await reconcile_deletions(store, fake_papra, fake_kz)
    await run_cleanup(store, fake_kz)
    assert fake_kz.deleted == [7]

async def test_failed_upstream_delete_retries_from_the_ledger_after_restart(fake_kz, store):
    """Crash-durable: the intent lives in the ledger, not in a variable."""
    fake_kz.fail_delete(7)
    await reconcile_deletions(store, fake_papra, fake_kz); await run_cleanup(store, fake_kz)
    assert store.ledger_get("org-a", "doc-1").cleanup_state == "pending"
    fake_kz.fail_delete(None)
    await run_cleanup(store, fake_kz)                    # a new process would do this
    assert fake_kz.deleted == [7]

async def test_cleanup_stops_erroring_after_five_attempts(fake_kz, store):
    seed_reference(store, deleted=True, kreuzakt_id=7)
    fake_kz.fail_delete(7)
    for _ in range(6):
        await run_cleanup(store, fake_kz)
    ref = store.ledger_get("org-a", "doc-1")
    assert ref.cleanup_state == "failed" and ref.cleanup_attempts == 5
    assert sum("cleanup failed permanently" in r.message for r in caplog.records) == 1


async def test_hard_deleted_documents_are_handled_by_listing_absence(fake_papra, store):
    """Papra can hard-delete; the ledger still references a document nobody can list."""
    seed_reference(store, kreuzakt_id=7)
    fake_papra.documents = []                     # the listing no longer contains it
    result = await reconcile_deletions(store, fake_papra, fake_kz)
    assert result.refs_marked == 1
    assert store.ledger_resolve([7], generation=1) == []


async def test_kreuzakt_side_deletion_never_touches_papra(fake_papra, fake_kz, store):
    """One-way propagation: an index-side delete makes it unfindable, nothing more."""
    seed_reference(store, kreuzakt_id=7)
    fake_kz.missing_documents = {7}
    await reconcile_deletions(store, fake_papra, fake_kz)
    assert fake_papra.patch_calls == 0 and fake_papra.delete_calls == 0
    assert store.ledger_needs_index(generation=1)          # the sweep re-indexes it


async def test_restore_needs_current_generation_before_it_can_resolve(fake_papra, fake_kz, store):
    seed_reference(store, deleted=True, kreuzakt_id=7)
    store.bump_generation()
    await reconcile_deletions(store, fake_papra, fake_kz)
    ref = store.ledger_get("org-a", "doc-1")
    assert ref.deleted_at is None and ref.delivered_generation != store.get_generation()
    assert store.ledger_needs_index(generation=store.get_generation())     # must be re-indexed
async def test_restore_while_cleanup_is_pending_cancels_the_upstream_delete(fake_kz, store):
    """Durable intent must not outlive the reason for it."""
    seed_reference(store, deleted=True, kreuzakt_id=7)
    await reconcile_deletions(store, fake_papra, fake_kz)
    await run_cleanup(store, fake_kz)                # queued, not yet executed?
    fake_papra.documents["doc-1"]["isDeleted"] = False
    store.ledger_mark_restored(papra_org_id="org-a", papra_document_id="doc-1")
    await run_cleanup(store, fake_kz)
    assert fake_kz.deleted == []
    assert store.ledger_get("org-a", "doc-1").cleanup_state == "skipped"


async def test_cleanup_never_deletes_a_reused_integer_after_a_rebuild(fake_kz, store):
    """The dangerous one: id 7 meant one document before the rebuild, another after."""
    seed_reference(store, deleted=True, kreuzakt_id=7)
    await reconcile_deletions(store, fake_papra, fake_kz)
    store.bump_generation()
    seed_ledger(store, document_id="doc-new", kreuzakt_id=7, generation=store.get_generation())
    await run_cleanup(store, fake_kz)
    assert fake_kz.deleted == []
    assert store.ledger_get("org-a", "doc-1").cleanup_state == "skipped"
    assert any("cleanup skipped: generation changed" in r.message for r in caplog.records)


async def test_a_failing_listing_aborts_the_whole_pass(fake_papra, store):
    """Never infer mass deletion from a paging error."""
    fake_papra.raise_on_list = True
    result = await reconcile_deletions(store, fake_papra, fake_kz)
    assert result.aborted and store.ledger_stats()["cleanup_pending"] == 0
async def test_deletion_pass_is_capped_per_pass(fake_papra, fake_kz, store):
    seed_deleted_in_papra(fake_papra, count=MAX_DELETIONS_PER_PASS + 5)
    result = await reconcile_deletions(store, fake_papra, fake_kz)
    assert result.capped == MAX_DELETIONS_PER_PASS
    assert any("deletion pass capped" in r.message for r in caplog.records)
```

- [ ] **Step 2: Implement with abort-before-act and a cap**

Walk each org's listing with `list_documents(page_index=…, page_size=100)` until the returned count drops below the page size; collect `isDeleted` documents and the set of ids seen. **Any exception during the walk discards the pass**: `logger.error("listing failed, skipping deletion pass org=%s page=%s", ...)` and `aborted=True`. Proposed marks are sorted by `papra_document_id` and truncated to `MAX_DELETIONS_PER_PASS`, logging `deletion pass capped at N, N remaining for next pass` at ERROR so a mass-deletion event is loud rather than efficient. For each mark: `ledger_mark_deleted` **first**, then `ledger_queue_cleanup` when the remaining count is 0. Ordering is the invariant: a crash between the two leaves an orphan index entry that a later pass retries, never a live reference pointing at a destroyed entry.

`run_cleanup` drains `ledger_cleanup_due(now=…, limit=50)` and **revalidates before acting**, because a
queued delete can outlive both its reason and its target's meaning: skip with `cleanup_state='skipped'`
and an INFO line if the reference is no longer deleted, if `cleanup_generation != store.get_generation()`,
or if `ledger_resolve([cleanup_kreuzakt_id], generation=current)` returns anything (a live reference still
needs that document). Only then call `delete_document(cleanup_kreuzakt_id)`, and record
`ledger_cleanup_result(ok=…, next_at=…)` with `defer = CLEANUP_BACKOFF_S * attempts`; on the fifth failure
log at ERROR naming the Kreuzakt id and stop retrying (`cleanup_state='failed'`), leaving the operator
`papra-suite ledger --cleanup` to force a retry.

- [ ] **Step 3: Hook both into the sweep and log the counts**

`sweep_once` calls `reconcile_deletions` then `run_cleanup` at the end and adds their counts to `SweepResult`; `check` shows `cleanup-pending`.

- [ ] **Step 4: Mutation check (d) and commit**

```bash
sed -i.bak 's/if remaining == 0:/if True:/' papra_suite/indexing.py
pytest tests/test_deletion_propagation.py::test_shared_document_survives_one_reference_being_deleted -x -q
git checkout -- papra_suite/indexing.py && rm -f papra_suite/indexing.py.bak
git add -A && git commit -m "feat(index): crash-durable reference-counted delete propagation"
```

---

### Task 10: Papra webhook receiver — **DROPPED, do not implement** (operator decision 2026-11-17)

> **Status: out of v1.** The operator accepted the cost of the six-hour `_verification_loop`
> (Task 7, `interval_s=21600`) as the propagation window for human edits and deletions made in
> Papra's own UI, in exchange for not standing up a new inbound HTTP listener with its own secret
> and its own fight with `webhookUrlAllowedHostnames`. Nothing else in this plan depends on the
> receiver: ownership is decided by Task 8's comparison, delete propagation by Task 9, and
> convergence by Task 7. **Skip this task.** The design below is kept as an archive so a revival
> does not have to be re-derived, and it is not free of defects: it originally claimed to mount on
> "the existing loopback dashboard listener", and no such listener exists — `config.py` parses
> `DASHBOARD_BIND` into `Config.dashboard_port` and nothing outside `config.py` reads that field,
> because the M4 dashboard was never built (spec §12 milestone 4). A revived receiver needs its own
> loopback bind inside `serve`, and must not be bolted onto the MCP server: that app is the
> read-only trust boundary and should not also accept inbound pushes.
>
> **Revival conditions.** Revisit only if a human-edit-in-Papra-latency complaint actually happens,
> and only with Task 3's measured answer to one question: does a `document.updated` payload name the
> actor at all? Without an actor field the receiver can only ever be a nudge (which is still safe,
> since it decides nothing); with one, it can stop re-fetching on our own writes.

Historic rationale, still true and still the reason the design is shaped this way: a webhook that treats "an update happened" as "a human edited this field" would protect our own titles and silently disable renaming across the corpus. So the receiver never decides ownership: it triggers the ordinary re-fetch-and-compare path, and ownership stays exactly where Task 8 put it. If Task 3 showed delivery is unusable, mark this task **skipped** with the evidence and continue — Tasks 8 and 9 stand alone.

**Files:** Create `papra_suite/webhooks.py`, `tests/test_webhooks.py`; Modify `papra_suite/pipeline.py`.

**Interfaces:**
- Consumes: the payload/signature measured in Task 3; `Store.dedup_exists/add_dedup` (existing) for replay suppression; `deliver_content`, `deliver_metadata`, `reconcile_deletions`.
- Produces (archived design, not built): `WebhookReceiver(store, papra, kz, *, secret) -> Starlette` with `POST /hooks/papra` on its own loopback bind. `WEBHOOK_EVENTS = {"document.updated", "document.deleted", "document.restored"}`.

- [ ] **Step 1: Write the failing tests**

(Not scheduled — no implementer runs the steps below.)

```python
async def test_forged_or_missing_signature_is_rejected_401(store, client):
    body = event_body("document.updated", document_id="doc-1")
    assert (await client.post("/hooks/papra", content=body,
        headers={"x-signature": "deadbeef"})).status_code == 401
    assert (await client.post("/hooks/papra", content=body)).status_code == 401
    assert store.ledger_get("org-a", "doc-1").name_protected is False


async def test_body_over_64kib_is_rejected_413(store, client):
    body = signed(b'{"event": "document.updated", "padding": "' + b"a" * 70000 + b'"}')
    assert (await client.post("/hooks/papra", content=body,
                              headers=sign_headers(body))).status_code == 413
async def test_replayed_event_id_is_acked_once_and_processed_once(store, client):
    """Replay suppression uses Store.add_dedup, so it survives a restart."""
async def test_update_event_refetches_and_uses_the_ordinary_ownership_rule(store, client, fake_papra):
    """The whole point: the event says 'look again', it does not say 'a human did this'."""
    fake_papra.document["name"] = "Human Title.pdf"
    await post_event(client, "document.updated", document_id="doc-1")
    assert store.ledger_get("org-a", "doc-1").name_protected is True     # via comparison
    assert fake_papra.patch_calls == 0
async def test_our_own_write_event_changes_nothing(store, client, fake_papra):
    fake_papra.document["name"] = "Rechnung 4711"      # the name we wrote last
    await post_event(client, "document.updated", document_id="doc-1")
    assert store.ledger_get("org-a", "doc-1").name_protected is False
async def test_deleted_event_marks_the_reference_without_deleting_shared_document(store, client,
                                                                                   fake_kz):
    seed_two_bucket_reference(store, kreuzakt_id=7)
    await post_event(client, "document.deleted", document_id="doc-1")
    assert store.ledger_get("org-a", "doc-1").deleted_at is not None
    assert fake_kz.deleted == []                    # the other bucket still needs it


async def test_unknown_event_is_acked_and_ignored(store, client):
    response = await post_event(client, "document.tags.changed", document_id="doc-1")
    assert response.status_code == 200
    assert store.ledger_get("org-a", "doc-1").name_protected is False
async def test_handler_acks_only_after_the_reconcile_is_enqueued(store, client, fake_papra):
    """Ack-first would drop the hint on a crash between ack and work."""
```

- [ ] **Step 2: Implement**

Verify `X-Signature`-equivalent per Task 3's finding (constant-time compare, body read once, capped at 64 KiB). Dedupe on the event id with `Store.dedup_exists("webhook:" + event_id)`. Dispatch: `document.updated` → `deliver_metadata` + `deliver_content` for that reference if it is in the ledger and current generation; `document.deleted` → the mark path from Task 9 for that one reference; `document.restored` → `ledger_mark_restored`. Everything else: 200 and ignore, so Papra's delivery log stays green and its operator does not learn to ignore red.

- [ ] **Step 3: Integration test on atlas**

Rename a document through Papra's API, then poll `ledger_get(...).name_protected` for up to 10 s with 250 ms waits (never a blind sleep), asserting it became `True` — the first end-to-end proof that a human rename is protected within the delivery window rather than at the next write.

- [ ] **Step 4: Commit**

```bash
git add papra_suite/webhooks.py tests/test_webhooks.py papra_suite/pipeline.py README.md
git commit -m "feat(webhooks): signed Papra events as reconciliation hints, never as proof"
```

---

### Task 11: Rebuild safety by generation, with re-index from Papra's bytes

v1 proposed matching stale ids to fresh documents via `file_hash` prefixes and our synthetic filename. Both are unsafe: 8 hex characters are not hash verification, and a deduplicated Kreuzakt document may carry another bucket's filename. This task replaces that with generation plus re-upload.

**Files:** Create `papra_suite/ledger_cli.py`, `tests/test_ledger_rebuild.py`; Modify `papra_suite/indexing.py`, `README.md`.

**Interfaces:**
- Consumes: `Store.get_generation/bump_generation/ledger_needs_index`, `KreuzaktClient.health`, `enqueue_index_job`.
- Produces: `async def reconcile_generation(store, kz) -> RebuildResult(generation: int, stale: int, requeued: int)`, CLI `papra-suite ledger (--status | --invalidate | --cleanup)`, where `--cleanup` forces one immediate pass over rows in `cleanup_state='failed'`.

- [ ] **Step 1: Write the failing tests**

```python
async def test_bumped_generation_serves_nothing(store, fake_kz):
    """An unnoticed volume restore must not be able to answer a search."""
    store.bump_generation()
    assert store.ledger_resolve([7], generation=store.get_generation()) == []

async def test_reconcile_requeues_stale_references_without_losing_protection(store, fake_kz):
    store.bump_generation()
    result = await reconcile_generation(store, fake_kz)
    assert result.requeued == 2
    ref = store.ledger_get("org-a", "doc-1")
    assert ref.name_protected is True and ref.papra_original_name == "scan-0001.pdf"

async def test_requeued_jobs_upload_from_papra_and_settle_on_new_ids(store, fake_kz, fake_papra):
    store.bump_generation()
    await reconcile_generation(store, fake_kz)
    job = store.claim_next_job("index")
    outcome = await run_index_job(fake_kz, store, job, papra=fake_papra, poll_s=0, wait_timeout_s=5)
    assert outcome.kreuzakt_id == fake_kz.high_water + 1
    assert store.ledger_get("org-a", "doc-1").delivered_generation == store.get_generation()

async def test_invalidate_is_refused_while_kreuzakt_is_down(store, fake_kz):
    """Invalidating against a dead index turns a bad moment into a silent search outage."""
    fake_kz.health = {"status": "error", "database": False}
    before = store.get_generation()
    with pytest.raises(RuntimeError, match="unreachable"):
        await ledger_cli.invalidate(store, fake_kz)
    assert store.get_generation() == before
async def test_two_shared_references_reupload_independently_and_dedupe_back(store, fake_kz):
    """Kreuzakt collapses them again; both rows end up on one current id."""
```

- [ ] **Step 2: Implement `--invalidate` as the only rebuild entry point**

`ledger --invalidate` refuses unless `kz.health()["status"] == "ok"` (invalidating against a dead index turns a bad moment into a silent outage of search), calls `bump_generation()`, then `reconcile_generation` re-enqueues `ledger_needs_index` documents through `enqueue_index_job`, so re-indexing reuses the intake path rather than a second implementation. `ledger --status` prints generation, references, current, stale, cleanup-pending, and protection counts.

- [ ] **Step 3: Write and time the runbook**

In `README.md`: stop the suite → wipe `kreuzakt-data` → start Kreuzakt → `papra-suite check` → `papra-suite ledger --invalidate` → `papra-suite backfill --dry-run` → the real re-index → `papra-suite ledger --status`. Record the **measured** wall-clock for the operator's 500 documents and the peak `extraction-empty` count; a runbook with an invented duration is decoration.

- [ ] **Step 4: Commit**

```bash
git add papra_suite/ledger_cli.py papra_suite/indexing.py tests/test_ledger_rebuild.py README.md
git commit -m "feat(index): generation-based rebuild safety; drop unsafe id remapping"
```

---

### Task 12: Token CLI and MCP server skeleton, with `index_status` scoped

Auth and transport come from `docs/superpowers/specs/2026-11-16-mcp-search-server-design.md` **verbatim**: `mcp` 2.x Streamable HTTP, `token_verifier` under `auth` settings, the Host/Origin allow-list answering **421** to a bare host, 401 for query-string tokens, no Funnel, pydantic returns, the error model. What changes is the token store: per-token rows carrying bucket lists, replacing that spec's single `mcp_allowed_org_ids` key.

**Files:** Create `papra_suite/mcp_tokens.py`, `papra_suite/mcp_server.py`, `tests/test_mcp_tokens.py`, `tests/test_mcp_server_auth.py`; Modify `papra_suite/__main__.py`, `papra_suite/pipeline.py`, the 2026-11-16 spec.

**Interfaces:**
- Consumes: `Store.get_setting/set_setting`, `ledger_stats`, `KreuzaktClient.queue_counts`.
- Produces:
  - `mint_token(store, *, label, org_ids, scope_all=False) -> str`; `list_tokens(store) -> list[TokenInfo]`; `revoke_token(store, *, label) -> bool`
  - `resolve_token(store, raw: str) -> TokenContext | None`, `TokenContext(label: str, org_ids: frozenset[str], scope_all: bool)`
  - `build_mcp_app(store, papra, kz, *, url: str) -> Starlette`
  - Settings key `mcp_token:<sha256hex>` → `{"label", "org_ids", "scope_all", "created_at", "revoked_at"}`

- [ ] **Step 1: Write the failing tests**

```python
def test_mint_stores_only_a_hash_and_returns_plaintext_once(store):
    token = mint_token(store, label="hermes", org_ids=["org-1"])
    assert token not in "".join(store.list_setting_keys() and
                                [store.get_setting(k) for k in store.list_setting_keys()])
    assert resolve_token(store, token).org_ids == frozenset({"org-1"})

def test_revoked_token_resolves_to_nothing(store):
    token = mint_token(store, label="pi", org_ids=["org-1"])
    assert revoke_token(store, label="pi") is True
    assert resolve_token(store, token) is None
    assert resolve_token(store, token + "x") is None      # no partial-match acceptance


def test_two_tokens_do_not_share_scope(store):
    hermes = mint_token(store, label="hermes", org_ids=["org-1", "org-2"])
    pi = mint_token(store, label="pi", org_ids=["org-3"])
    assert resolve_token(store, hermes).org_ids == frozenset({"org-1", "org-2"})
    assert resolve_token(store, pi).org_ids == frozenset({"org-3"})


def test_wrong_and_missing_token_return_the_same_401(client):
    no_key = client.get("/mcp", headers={"host": "papra-mcp.ts.net"})
    bad_key = client.get("/mcp", headers={"host": "papra-mcp.ts.net",
                                         "authorization": "Bearer nope"})
    assert no_key.status_code == bad_key.status_code == 401
    assert no_key.text == bad_key.text            # no oracle for "token exists"


def test_query_string_token_is_rejected(client):
    response = client.get("/mcp?token=abc", headers={"host": "papra-mcp.ts.net"})
    assert response.status_code == 401            # tokens never travel in URLs


def test_bare_host_gets_421(client):
    """The 11-16 spec's deployment blocker, kept: a bare Host never reaches MCP code."""
    assert client.post("/mcp", headers={"host": "100.64.1.20:8765", "content-type":
                       "application/json"}, json={}).status_code == 421


def test_mcp_url_must_be_configured_or_startup_fails(config):
    with pytest.raises(ValueError, match="PAPRA_MCP_URL"):
        build_mcp_app(store, papra, kz, url=config.mcp_url)     # mcp_url == ""
async def test_index_status_for_a_scoped_token_leaks_no_global_counts(store, client):
    """Mutation target: global queue counts to a one-bucket token is a leak."""
    seed(queue_counts={"pending": 40, "completed": 900}, refs=5)
    status = await call_tool(client, token="scoped", name="index_status")
    assert status.queue is None and status.indexed_for_scope == 5
async def test_index_status_for_an_all_bucket_token_includes_queue(store, client):
    seed(queue_counts={"pending": 40, "completed": 900}, refs=5)
    status = await call_tool(client, token="all_buckets", name="index_status")
    assert status.queue["pending"] == 40 and status.queue["completed"] == 900
async def test_index_status_raises_index_unavailable_when_kreuzakt_is_down(store, client):
    """No Papra-backed fallback anywhere: an outage says 'index unavailable'."""
```

- [ ] **Step 2: Implement `index_status` with the scoping rule**

```python
@server.tool()
async def index_status() -> IndexStatus:
    """Index state. pending > 0 means 'still being indexed', which is a different
    answer from 'not in your archive' and must not be conflated."""
    ctx = current_token_context()
    scoped = store.ledger_stats(org_ids=None if ctx.scope_all else sorted(ctx.org_ids))
    queue = await kz.queue_counts() if ctx.scope_all else None
    return IndexStatus(indexed_for_scope=scoped["indexed_current"],
                       extraction_empty=scoped["extraction_empty"],
                       queue=queue)
```
Queue depth is corpus-wide, so it is privileged: it is returned only to a token minted with `--all-buckets`, which is how both of the operator's tokens are created (decision §13.5) while the mechanism still supports a scoped one.

- [ ] **Step 3: CLI with the printing discipline**

`mcp-token --create --label pi [--org-id ID ...] [--all-buckets]` prints the plaintext once between two warnings that it will not be shown again, and writes no log line containing it. `--list` prints label, first 8 chars of the hash, buckets or `ALL`, created, revoked. `--revoke --label pi` prints how many active sessions are moot. `check` and `papra-suite events` must never print a token; assert that in a test.

- [ ] **Step 4: Integration test with the SDK's own Streamable HTTP client** on atlas, mirroring the 2026-11-16 spec's list (initialize, session id reuse, 401, 421, and — new — the Host/Origin values that `tailscale serve` forwards, recorded in the spec's Operations section rather than assumed).

- [ ] **Step 5: Commit**

```bash
git add papra_suite/mcp_tokens.py papra_suite/mcp_server.py tests/test_mcp_tokens.py tests/test_mcp_server_auth.py docs/superpowers/specs/2026-11-16-mcp-search-server-design.md
git commit -m "feat(mcp): per-token bucket scopes and scoped index status"
```

---

### Task 13: `search_documents` and `list_recent_documents` — the authorization boundary

**Files:** Create `papra_suite/mcp_tools.py`, `tests/test_mcp_search.py`, `tests/fixtures/kreuzakt_search_page.json`; Modify `papra_suite/mcp_server.py`.

**Interfaces:**
- Consumes: `KreuzaktClient.search/list_documents`, `Store.ledger_resolve`, `PapraClient.get_document`, `TokenContext`.
- Produces:
  - `SearchItem(document_id: str, org_id: str, title: str, document_date: str | None, added_at: str | None, original_filename: str)`
  - `SearchPage(items: list[SearchItem], page: int, page_size: int, partial: bool, scanned: int, stats: ResolveStats)`
  - `async def resolve_hits(ctx, store, papra, hits, *, generation) -> tuple[list[SearchItem], ResolveStats(kept, dropped_unauthorized, dropped_deleted, dropped_lookup_error, expanded)]`
  - `async def search_documents(ctx, store, papra, kz, query, *, page=1, page_size=20) -> SearchPage`
  - `async def list_recent_documents(ctx, store, papra, kz, *, limit=20) -> list[SearchItem]`
  - Bounds: `1 <= page_size <= 50`, `page >= 1`, `len(query.strip()) <= config.index_query_max_chars`; violations raise the tool's `invalid-request` error, they are never clamped silently.
  - Stop condition: keep pulling Kreuzakt pages until **either** `len(items) >= page * page_size`, **or** upstream is exhausted (`len(hits) < limit` **or** `scanned >= total`), **or** the scan budget (`config.index_scan_pages × 100`, hard-capped by `index_max_scan_rows`) is spent.
  - `partial = True` only when the budget ran out before the request was satisfied **or** exhaustion was proven. A full page that also exhausted upstream is `partial=False`.
  - A hit resolving to N authorized references produces N items, in Kreuzakt hit order, deduplicated by `(org_id, document_id)` — which is unique per reference by construction.

- [ ] **Step 1: Write the failing tests**

```python
async def test_four_buckets_four_tokens_see_nothing_foreign(store, fake_kz, fake_papra):
    """Mutation check (a): drop the ledger filter and this dies."""
    seed_four_buckets(store, fake_kz, fake_papra)
    for label, org in scopes():
        page = await search_documents(ctx(label), store, fake_papra, fake_kz, "rechnung")
        assert {i.org_id for i in page.items} == {org}
        assert all(i.document_id in allowed[label] for i in page.items)

async def test_hit_expanding_to_two_authorized_references_yields_two_items(store, fake_kz):
    """Same bytes in two buckets of one token: one Kreuzakt hit, two Papra docs."""
    page = await search_documents(ctx_all, store, fake_papra, fake_kz, "rechnung")
    assert [i.document_id for i in page.items] == ["doc-1", "doc-2"]
    assert stats.expanded == 1

async def test_page_size_multiple_of_100_stops_at_exhaustion_not_budget(store, fake_kz):
    fake_kz.search_pages = 1                     # upstream has exactly 100 hits
    page = await search_documents(ctx, store, fake_papra, fake_kz, "miete", page_size=50)
    assert page.partial is False and page.scanned == 100

async def test_full_page_from_a_dominant_bucket_still_fills_the_callers_page(store, fake_kz):
    """99 hits from a bucket this token cannot see, then five it can: page one is full."""
    foreign = [hit(900 + i) for i in range(99)]
    for i in range(1, 6):                                   # five authorized Papra docs
        seed_ledger(store, document_id=f"doc-own-{i}", kreuzakt_id=i, generation=1)
        fake_papra.documents[f"doc-own-{i}"] = papra_doc(document_id=f"doc-own-{i}")
    fake_kz.hits = foreign + [hit_for(f"doc-own-{i}") for i in range(1, 6)]
    page = await search_documents(ctx_scoped_to("org-a"), store, fake_papra, fake_kz,
                                 "versicherung", page=1, page_size=5)
    assert [i.document_id for i in page.items] == [f"doc-own-{i}" for i in range(1, 6)]
    assert page.partial is False and page.scanned == 104


async def test_budget_exhaustion_reports_partial_and_counts_scanned(store, fake_kz):
    fake_kz.hits = [hit(i) for i in range(1, 901)]      # 9 pages of other buckets
    page = await search_documents(ctx, store, fake_papra, fake_kz, "versicherung",
                                 page=1, page_size=20)
    assert page.items == [] and page.partial is True
    assert page.scanned == 500                          # the configured row budget
    assert "partial" in describe(page)


async def test_page_past_the_end_is_an_empty_page_not_an_error(store, fake_kz):
    page = await search_documents(ctx, store, fake_papra, fake_kz, "miete", page=9, page_size=20)
    assert page.items == [] and page.page == 9


async def test_hit_with_no_ledger_row_is_invisible(store, fake_kz):
    """Kreuzakt knows a document we never uploaded, or already forgot. Fails closed."""
    fake_kz.hits = [hit(4242)]
    assert (await search_documents(ctx, store, fake_papra, fake_kz, "miete")).items == []


async def test_reference_to_a_deleted_papra_document_is_dropped(store, fake_papra):
    fake_papra.documents["doc-1"]["isDeleted"] = True
    page = await search_documents(ctx, store, fake_papra, fake_kz, "rechnung")
    assert page.items == [] and stats_of(page).dropped_deleted == 1


async def test_papra_lookup_error_drops_the_item_and_is_counted(store, fake_papra):
    fake_papra.raise_on_get = True
    page = await search_documents(ctx, store, fake_papra, fake_kz, "rechnung")
    assert page.items == [] and stats_of(page).dropped_lookup_error == 1


async def test_search_uses_the_captured_real_payload(store, fake_kz):
    """Against the byte-exact response from Task 3, not a hand-written dict."""
    fake_kz.raw_search = load("kreuzakt_search_page.json")
    page = await search_documents(ctx_all, store, fake_papra, fake_kz, "rechnung")
    assert [i.original_filename for i in page.items] == ["papra-doc-1-a1b2c3d4.pdf"]


async def test_oversized_query_and_page_size_are_rejected(store):
    for kwargs in ({"query": "x" * 513}, {"query": "  "}, {"query": "a", "page_size": 51},
                   {"query": "a", "page": 0}):
        with pytest.raises(ToolError, match="invalid-request"):
            await search_documents(ctx, store, fake_papra, fake_kz, **kwargs)
async def test_hit_on_wrong_generation_is_invisible(store, fake_kz):
    store.bump_generation()
    assert (await search_documents(ctx, store, fake_papra, fake_kz, "x")).items == []
async def test_deleted_in_papra_is_dropped_from_results(store, fake_papra):
    fake_papra.documents["doc-1"]["isDeleted"] = True
    page = await search_documents(ctx, store, fake_papra, fake_kz, "rechnung")
    assert page.items == [] and stats_of(page).dropped_deleted == 1
async def test_papra_lookup_error_drops_the_item_and_is_counted(store, fake_papra):
    fake_papra.raise_on_get = True
    page = await search_documents(ctx, store, fake_papra, fake_kz, "rechnung")
    assert page.items == [] and stats_of(page).dropped_lookup_error == 1
async def test_live_papra_state_is_checked_only_for_items_being_returned(store, fake_papra):
    assert fake_papra.get_calls <= len(returned) + 1     # not once per scanned hit
async def test_json_contains_no_kreuzakt_path_or_integer_id(store, fake_kz):
    blob = (await search_documents(ctx, store, fake_papra, fake_kz, "rechnung")).model_dump_json()
    for needle in ("/api/documents/", "/documents/", "/thumbnail", "/original", "kreuzakt"):
        assert needle not in blob
```

- [ ] **Step 2: Implement the loop and the single shared resolver**

```python
async def resolve_hits(ctx, store, papra, hits, *, generation):
    """One ledger query per page, never one per hit (spec §4.5 rule 2)."""
    refs = store.ledger_resolve([hit.id for hit in hits], generation=generation)
    by_kreuzakt: dict[int, list[LedgerRef]] = {}
    for ref in refs:
        if ctx.scope_all or ref.papra_org_id in ctx.org_ids:
            by_kreuzakt.setdefault(ref.kreuzakt_id, []).append(ref)
    items, stats = [], ResolveStats(0, 0, 0, 0, 0)
    for hit in hits:                                  # Kreuzakt BM25 order kept
        candidates = by_kreuzakt.get(hit.id, [])
        stats.dropped_unauthorized += int(hit.id not in by_kreuzakt)
        for ref in candidates:
            live = await papra.get_document(ref.papra_org_id, ref.papra_document_id)  # cached
            if live is None:
                stats.dropped_lookup_error += 1       # fail closed
            elif live.get("isDeleted"):
                stats.dropped_deleted += 1
            else:
                stats.kept += 1
                items.append(to_item(ref, hit, live))
    return items, stats
```
Cache `papra.get_document` for the duration of the call in a dict — the invariant is that authorization reflects Papra's current state, and a cache scoped to one request keeps that honest without 500 round trips. `search_documents` runs the paging loop from the stop conditions above and slices `[start:start + page_size]` at the end, so filtering never shifts Kreuzakt's ordering.

- [ ] **Step 3: `list_recent_documents`** pulls `/api/documents?page=1&limit=100` (and further pages under the same budget), then the same `resolve_hits`; it invents no local ordering, sorting only by Kreuzakt's `added_at` order as returned.

- [ ] **Step 4: Run mutation check (a), gates, commit**

```bash
sed -i.bak 's/if ctx.scope_all or ref.papra_org_id in ctx.org_ids:/if True:/' papra_suite/mcp_tools.py
pytest tests/test_mcp_search.py::test_four_buckets_four_tokens_see_nothing_foreign -x -q
git checkout -- papra_suite/mcp_tools.py && rm -f papra_suite/mcp_tools.py.bak
make lint && make test-unit
git add -A && git commit -m "feat(mcp): search and list filtered by the ledger, fail-closed and bounded"
```

---

### Task 14: `get_document`, `get_document_content`, `open_document`

**Files:** Modify `papra_suite/mcp_tools.py`; Create `tests/test_mcp_documents.py`.

**Interfaces:**
- Consumes: `Store.ledger_get/get_generation`, `KreuzaktClient.get_document`, `Config.papra_base_url`, the sanitiser rule from `ingest_events` (isprintable + category filter).
- Produces:
  - `async def get_document(ctx, store, papra, kz, document_id: str) -> DocumentView(document_id, org_id, title, description, document_date, original_filename, mime_type, page_count, opened_from: str)`
  - `async def get_document_content(ctx, store, papra, kz, document_id, *, max_chars=20000) -> ContentResult(text, truncated, chars_total, sanitized: bool)`
  - `async def open_document(ctx, store, document_id) -> OpenResult(viewer_url, download_url)`
  - `ToolError(code)` with codes `not-found`, `not-in-scope`, `index-unavailable`, `invalid-request`.

- [ ] **Step 1: Write the failing tests**

```python
async def test_client_ids_are_papra_ids_and_kreuzakt_ids_are_rejected(store):
    with pytest.raises(ToolError, match="not-found"):
        await get_document(ctx, store, fake_papra, fake_kz, "7")     # a Kreuzakt integer
async def test_reference_outside_the_token_is_not_in_scope(store, fake_papra, fake_kz):
    seed_ledger(store, org_id="org-b", document_id="doc-b", kreuzakt_id=9)
    with pytest.raises(ToolError, match="not-in-scope"):
        await get_document(ctx_scoped_to("org-a"), store, fake_papra, fake_kz, "doc-b")
    assert fake_kz.calls == []                 # refused before reaching the index


async def test_document_with_no_ledger_row_fails_closed(store, fake_papra, fake_kz):
    with pytest.raises(ToolError, match="not-found"):
        await get_document(ctx, store, fake_papra, fake_kz, "doc-unknown")
    assert fake_kz.calls == []


async def test_deleted_in_papra_is_not_found_even_though_the_index_still_has_it(store, fake_papra):
    seed_ledger(store, kreuzakt_id=7)
    fake_papra.documents["doc-1"]["isDeleted"] = True
    with pytest.raises(ToolError, match="not-found"):
        await get_document_content(ctx, store, fake_papra, fake_kz, "doc-1")
async def test_content_is_capped_and_says_so(store):
    result = await get_document_content(ctx, store, fake_papra, fake_kz, "doc-1", max_chars=100)
    assert result.truncated and result.chars_total > 100
async def test_open_document_urls_are_papra_only_and_require_no_kreuzakt_call(store, fake_kz):
    result = await open_document(ctx, store, "doc-1")
    assert result.viewer_url.startswith("http://papra:1221/organizations/org-a/documents/doc-1")
    assert fake_kz.calls == []
async def test_extracted_text_is_wrapped_as_untrusted_content(store):
    assert "<document-content" in (await get_document_content(...)).text
async def test_control_and_format_characters_are_stripped(store):
    assert "\x02" not in result.text and "\u202e" not in result.text
async def test_kreuzakt_unreachable_raises_index_unavailable(store, fake_papra, fake_kz):
    """No Papra-backed fallback: an outage says so instead of returning the worse copy."""
    fake_kz.error = KreuzaktError("connection refused")
    with pytest.raises(ToolError, match="index-unavailable"):
        await get_document_content(ctx, store, fake_papra, fake_kz, "doc-1")
```

- [ ] **Step 2: Implement, resolving Papra → Kreuzakt → Papra**

Accept a **Papra** id, load the reference with `ledger_get`, require `not deleted_at`, `delivered_generation == store.get_generation()`, and `ctx.scope_all or org_id in ctx.org_ids`, then check Papra's live state, then fetch Kreuzakt content by `ref.kreuzakt_id`. Never accept a Kreuzakt id: making its integers client-visible is the identity model this design refuses. `open_document` builds `{papra_base_url}/organizations/{org}/documents/{id}` and `…/file` and touches no other service.

- [ ] **Step 3: Commit**

```bash
git add papra_suite/mcp_tools.py tests/test_mcp_documents.py
git commit -m "feat(mcp): document tools with Papra-only identities and untrusted-text wrapping"
```

---

### Task 15: `backfill` and `jobs` CLIs

**Files:** Create `papra_suite/jobs_cli.py`, `papra_suite/backfill_cli.py`, `tests/test_jobs_cli.py`, `tests/test_backfill_cli.py`; Modify `papra_suite/__main__.py`, `papra_suite/store.py` (`count_jobs`).

**Interfaces:**
- Consumes: `Store.list_jobs/count_jobs/reset_job/enqueue_job`, `ledger_needs_index`, `PapraClient.list_documents`, `_is_pdf`.
- Produces: `papra-suite jobs [--kind K] [--status S] [--limit 50]`, `papra-suite jobs --reset JOB_ID`, `papra-suite jobs --kind K --drain`, `papra-suite backfill [--org-id ID] [--limit N] [--dry-run] [--all-stale]`.
- Eligibility: a document is enqueued when the ledger has **no current-generation reference** for it (`--all-stale` walks `ledger_needs_index`), **not** when `document_seen` says we have ingested it — that flag records ingestion, so after a rebuild every document would look already-done.

- [ ] **Step 1: Write the failing tests**

```python
def test_jobs_lists_kind_org_document_attempts_error(capsys, store):
    job_id = store.enqueue_job(kind="index", papra_org_id="org-a", document_id="doc-1",
                              content_hash=H, payload={})
    store.mark_job_failed(job_id, "kreuzakt queue error: extraction died", max_attempts=1)
    assert main(["jobs", "--kind", "index", "--status", "failed"]) == 0
    out = capsys.readouterr().out
    for needle in ("index", "org-a", "doc-1", "extraction died"):
        assert needle in out


def test_reset_clears_error_and_makes_the_job_claimable(store):
    job_id = failing_index_job(store, attempts=3)
    assert store.claim_next_job("index") is None         # attempts exhausted
    assert main(["jobs", "--reset", job_id]) == 0
    assert store.get_job(job_id).status == "pending"
    assert store.claim_next_job("index").id == job_id


def test_reset_unknown_id_exits_2_and_writes_nothing(store, capsys):
    assert main(["jobs", "--reset", "nope"]) == 2
    assert "unknown job" in capsys.readouterr().err
    assert store.list_jobs() == []


def test_drain_refuses_while_any_job_is_pending_or_running(store, capsys):
    seed_job(store, kind="ocr", status="pending")
    seed_job(store, kind="ocr", status="running")
    assert main(["jobs", "--kind", "ocr", "--drain"]) == 1
    assert "cannot drain" in capsys.readouterr().out


def test_drain_succeeds_when_queue_is_quiet(store, capsys):
    assert main(["jobs", "--kind", "ocr", "--drain"]) == 0
    assert "safe to switch" in capsys.readouterr().out


def test_backfill_dry_run_counts_without_enqueuing(store, fake_papra, capsys):
    seed_listing(fake_papra, count=12)
    assert main(["backfill", "--org-id", "org-a", "--dry-run"]) == 0
    assert fake_papra.list_calls and store.list_jobs() == []
    assert "would enqueue 12" in capsys.readouterr().out


def test_backfill_skips_documents_already_current_in_the_ledger(store, fake_papra, capsys):
    seed_listing(fake_papra, count=10)
    for i in range(10):
        seed_ledger(store, document_id=f"doc-{i}", kreuzakt_id=i + 1,
                   generation=store.get_generation())
    assert main(["backfill", "--org-id", "org-a"]) == 0
    assert store.list_jobs() == []
    assert "0 of 10" in capsys.readouterr().out


def test_backfill_requeues_everything_after_a_generation_bump(store, fake_papra):
    """The rebuild case v1 would have silently skipped: seen != indexed."""
    seed_listing(fake_papra, count=12)
    for i in range(12):
        seed_ledger(store, document_id=f"doc-{i}", kreuzakt_id=i + 1, generation=1)
        store.record_document_seen(papra_org_id="org-a", papra_document_id=f"doc-{i}",
                                  name="x", content_hash=H)
    store.bump_generation()
    assert main(["backfill", "--org-id", "org-a", "--all-stale"]) == 0
    assert len(store.list_jobs()) == 12


def test_backfill_ignores_non_pdf_documents(store, fake_papra):
    seed_listing(fake_papra, count=4, mime="image/png")
    assert main(["backfill", "--org-id", "org-a"]) == 0
    assert store.list_jobs() == []


def test_backfill_respects_limit_and_reports_remaining(store, fake_papra, capsys):
    seed_listing(fake_papra, count=12)
    assert main(["backfill", "--org-id", "org-a", "--limit", "5"]) == 0
    assert len(store.list_jobs()) == 5
    assert "7 remaining" in capsys.readouterr().out


def test_backfill_refuses_a_missing_database_before_opening_the_store(tmp_path, capsys):
    assert main(["--db", str(tmp_path / "absent.sqlite"), "backfill"]) == 2
    assert "database" in capsys.readouterr().err
    assert not (tmp_path / "absent.sqlite").exists()
```

- [ ] **Step 2: Implement** — walk each org's listing at `page_size=100`, filter with the existing `_is_pdf`, apply the eligibility rule, call `enqueue_index_job` for each (same crash-ordered path as intake; no second implementation), and print `enqueued N of M candidates across K buckets; L remaining` plus a reminder that each document costs VLM time.

- [ ] **Step 3: Commit**

```bash
git add papra_suite/jobs_cli.py papra_suite/backfill_cli.py papra_suite/__main__.py papra_suite/store.py tests/test_jobs_cli.py tests/test_backfill_cli.py
git commit -m "feat(cli): backfill and jobs repair surface; reset_job finally has a caller"
```

---

### Task 16: Backfill on atlas, verify German search, then delete the old workers

Run by a person at a keyboard, on the real archive. Deleting the workers before the corpus is re-extracted removes the fallback along with the evidence.

**Files:** Delete `papra_suite/ocr.py`, `papra_suite/titles.py`, `tests/test_ocr.py`, `tests/test_titles.py`; Modify `papra_suite/pipeline.py`, `papra_suite/config.py`, `README.md`, `docs/superpowers/HANDOFF.md`, spec §9/§14.

- [ ] **Step 1: Content delivery on, one bucket at a time**

```bash
ssh atlas 'cd ~/papra-ai-assistant && papra-suite jobs --kind ocr --drain'
# set PAPRA_INDEX_CONTENT_DELIVERY=true in the suite env, recreate the container
ssh atlas 'papra-suite backfill --org-id <bucket> --limit 20 --dry-run'
ssh atlas 'papra-suite backfill --org-id <bucket> --limit 20'
```
Read those twenty documents in Papra's UI. German must be correct: `Straße`, `Grüße`, `1.234,56`, IBANs, names. `check`'s `extraction-empty` must be small. **If it is large, stop and roll back the flag** — that is §9's signal that the extractor is wrong for this corpus, and at this point rollback is one flag because the old code still exists.

- [ ] **Step 2: Metadata delivery on, then inspect the ownership decisions**

```bash
ssh atlas 'papra-suite jobs --kind title --drain'
ssh atlas 'papra-suite metadata --restore-auto --dry-run'
```
`papra-suite check` must show a plausible `protected_name` count: near zero on a corpus nobody has renamed, and specifically non-zero for documents the operator knows he renamed. If protection fired everywhere, the baseline is wrong — fix `papra_original_name` capture before continuing, because the alternative is that renaming quietly stopped.

- [ ] **Step 3: Finish the corpus and assert the search class that motivated all of this**

Backfill the remaining buckets, then in the integration suite assert via MCP `search_documents` that a German scan is found for `Straße` **and** for an inflected form (`Rechnungen`-class) — the two query classes Papra's index could not serve. Record the measured queries and hit counts in spec §9 as the first real evidence for the unmeasured-quality decision.

- [ ] **Step 4: Delete the workers, and prove nothing refers to them**

```bash
git rm papra_suite/ocr.py papra_suite/titles.py tests/test_ocr.py tests/test_titles.py
grep -rn "run_ocr_job\|run_title_job\|enqueue_ocr_job\|kind=\"ocr\"\|kind=\"title\"\|OCR_MAX_PAGES\|ocr_dpi" papra_suite tests README.md
```
Expected: no matches. Remove the loops, the `on_job_done` OCR→title transition, and now-unused `Config` fields — checking each with grep rather than assuming, and keeping `llm_base_url`/`llm_api_key` only if something still uses them. Existing `ocr`/`title` rows in a live `jobs` table are inert; say so in the README, and do not delete rows from a store as part of a code change.

- [ ] **Step 5: Full verification, then commit**

```bash
make lint && make test-unit
ssh atlas 'cd ~/papra-ai-assistant && PAPRA_REQUIRE_STACK=1 make test-integration'
```
Expected green, with the integration count lower by exactly the deleted worker tests — state the number in the commit message.
```bash
git add -A && git commit -m "refactor!: remove our OCR and title workers; Kreuzakt extracts"
```
Then rewrite `HANDOFF.md`: the topology, the two delivery flags and their drain-first rule, the generation command for rebuilds, where the four executed mutation checks live, and what remains unmeasured.

---

## Spec coverage

| spec section | task |
| --- | --- |
| §4.1 extraction stays on, `deu,eng`, convergence | Task 2 (compose mirror), Task 7 (convergence, suppression) |
| §4.2 bridge steps 1–6, contract facts, unique filenames, correlation, blank-page rule | Tasks 3, 5, 6, 7 |
| §4.3 name/date/notes ownership, Papra-side baselines, escape hatch | Task 8 (ownership lives there alone; the dropped Task 10 was only ever a trigger) |
| §4.4 Kreuzakt config, no published port, Dokploy placement | Task 2 |
| §4.5 six tools, Papra-only links, no pass-through | Tasks 12, 13, 14 |
| §4.6 deletions from our codebase | Task 16 |
| §5 invariants 1–7 | Tasks 2 (1), 13 (2, 7), 7 (3), 8 (4), 9 (5), 6 (6) |
| §6 failure table | 6 (index down, queue failed), 7 (PATCH lost, rescan), 9 (deletes), 11 (rebuild), 12 (token leak) |
| §7 data model, generation | Tasks 4, 11 |
| §8 test plan | Tasks 4, 6, 7, 8, 9, 11, 13, 14; integration in 6, 7, 10, 12 |
| §9 quality risk and the extraction-empty signal | Tasks 6 (counting), 7 (`check`), 16 (steps 1 and 3) |
| §10 webhooks, date/notes quality | Tasks 3, 16 (Task 10 dropped 2026-11-17; webhooks measured for the record only) |
| §11 sequencing incl. drain-before-flip | task order 1 → 16, with drains in 7, 8, 16 |
| §14 tokens by CLI, no dashboard | Task 12 |

## Advisor review: what changed in v2, and what stays open

The review rejected v1 as executable and named six defects. Each is resolved above:

| finding | resolution |
| --- | --- |
| The convergence pass could not detect Papra overwriting us (`delivered_sha` shortcut) | Task 7 compares Papra's current content to Kreuzakt's current content; `test_papra_overwriting_us_is_detected_and_repaired` is the proof |
| Queue correlation guessed at an unmeasured contract | Task 3 measures pagination/ordering/retention; Task 5 correlates by pre-upload high-water mark with exact filename equality, and reports ambiguity instead of picking a winner; Task 6 refuses to deliver when ambiguous |
| Compose contradicted itself; `DOCKER_HOST=ssh://atlas` does not host the test process | Task 2: one network, no `internal:`, no `testnet`, a **test-only** loopback publication stated as a deviation, and integration tests run as a process on atlas |
| `original_filename` used as an ownership baseline while being our synthetic name | Task 4 adds `papra_original_name` captured at first sight; Task 8 tests that a synthetic name in Papra is protected, never claimed |
| Date/notes ownership undefined; truncation recorded the wrong value; no un-protect interface | Task 8: first write requires an empty Papra value, dates normalised with blanks skipped, `written_notes` records what was sent, `ledger_clear_protection` + `metadata --force --yes-i-mean-it` |
| Filename/hash-prefix remap on rebuild was unsafe | Task 11 replaces it with generation + re-index from Papra's bytes; a stale generation serves nothing |
| Re-index would silently skip everything (`document_seen` misuse) | Tasks 6/15 derive eligibility from the ledger's generation, never from ingestion history |
| Delete propagation needed durable retry, and mass-deletion safety | Task 9: ledger cleanup state machine carrying the target id **and its generation**, revalidated before every delete, five attempts, abort-on-listing-failure, per-pass cap with a loud log |
| In-flight jobs could bind a stale Kreuzakt id after a rebuild | Task 6 fences the write (`only_if_generation`), with `test_generation_change_mid_flight_discards_the_result` |
| A queued delete could destroy a reused integer id, or outlive a restore | Task 9: `cleanup_state='skipped'` when the reference is restored, the generation moved, or a live reference still resolves to that id |
| Writer handover needed a gate, not an empty-queue glance | Tasks 5/7/8: `PAPRA_LEGACY_WORKERS=false` stops the loops and the producers first, drain proves quiescence, and `deliver_content`/`deliver_metadata` raise `DeliveryDisabled` at the shared boundary so no path bypasses it |
| Queue scan could report a partial high-water mark | Task 5 raises instead of returning an unproven mark; Task 3 question 10 must confirm id monotonicity and ordering before Task 5 is written |
| One impossible test, and undeclared fixture names | Task 13's dominant-bucket test now seeds five authorized references; `fake_papra`/`fake_kz`/`_fill`/`papra_doc` are declared in the scaffolding table |
| `index_status` leaked corpus-wide counts to scoped tokens | Task 12 returns queue counts only to `--all-buckets` tokens |
| Paging: `partial` misreported, many-to-one expansion unspecified, no input bounds | Task 13 states stop conditions, the definition of `partial`, expansion/dedup rules, bounds, and tests for exact multiples of 100 and pages past the end |
| Webhooks could protect our own writes | Task 10 demotes events to reconciliation hints; ownership decisions stay in Task 8's comparison; replay suppression via `Store.add_dedup` — the receiver itself was later dropped, and these three rules are the conditions any revival must keep |
| House style: literal `...`, guessed payloads, mental mutation checks | Fixtures come from Task 3's captures instead of invented field names; every mutation check is a `sed` + `pytest` command pair; no placeholder test bodies remain — each named test has assertions in it |

Still open, and honestly so: Kreuzakt's German quality against omlx (Task 16 Step 1 is the first real evidence, and rollback is cheap until Step 4); queue retention beyond a few thousand rows; Papra actor attribution if the probe cannot find it; and upstream durability — the exit is re-indexing from Papra, not swapping indexers in place.

**Status of this plan.** A second review round added the fencing and handover items now in Tasks 5–9
(conditional id binding, generation-aware cleanup, the `DeliveryDisabled` boundary, `legacy_workers`
quiescence, the fail-closed queue scan). They are **specified, not executed**: no code in this plan has
been written or run, so no test named here has passed. Only Task 1 is safe to carry out before those
tests exist, because it changes nothing but branch topology — and no flag that lets the bridge write to
Papra should be flipped before the race-focused tests in Tasks 6–9 are green.
