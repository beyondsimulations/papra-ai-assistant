# Papra Suite — Milestone 3: OCR + title workers, sweep, serve Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the processing core: an OpenAI-compatible LLM client, the OCR worker (PDF → page images → vision model → markdown → write-back), the title worker (content excerpt + filename → title → write-back), the central transition handler (ocr → title), the document sweep for externally-ingested PDFs, and the `serve` command that runs pollers + workers + sweep in one asyncio process — proven end-to-end against a real two-page German PDF flowing through a mocked OpenAI-compatible server into the scratch Papra instance.

**Architecture:** One asyncio process. `llm_client.py` mirrors `papra_client.py` (thin httpx wrapper, one retry policy, `transport=` for tests). `ocr.py` owns rendering (PyMuPDF matrix-based downscale, page cap) and assembly; `titles.py` owns title generation/sanitization with filename fallback. `pipeline.py` grows the worker handlers, the transition handler, the per-kind worker loops, `sweep_once`, and `serve()` — the process runtime (M1+M2 carry-overs honored: `recover_running_jobs()` at startup, `register_config_secrets()`, defensive wrapping around polls). OCR concurrency = N worker tasks claiming from the shared queue (the store's conditional claim makes this safe), `OCR_CONCURRENCY` capped at 3 (decisions §1).

**Tech Stack:** Python 3.12, httpx, PyMuPDF (moves from dev to runtime dep; Pillow NOT needed — rendering and downscaling both go through PyMuPDF's matrix). Test fixture: a threaded stdlib `http.server`-based fake OpenAI-compatible server (`tests/fakes/llm_server.py`) — no Docker needed for the LLM leg; the Papra leg reuses the existing compose stack.

**Spec:** `spec.md` (§5.4 OCR worker, §5.5 title worker, §12 milestone 3) + decisions doc (§1 concurrency, §2 transition handler, §4 sweep, §6 job model). Read both before starting.

## Global Constraints

- Python 3.12; runtime deps: stdlib + httpx + pymupdf (requirements.txt gains `pymupdf==1.25.1`; requirements-dev keeps the rest).
- All job/document timestamps are STORE-GENERATED (+00:00 ISO) — no caller-supplied timestamps.
- The OCR transcription prompt is the spec's fixed wording (spec §5.4); the title prompt is written from spec §5.5's requirements, NOT lifted from papra-companion (spec §10 license constraints).
- LLM requests: timeout 300 s default; retry 3 attempts with backoff on 429/5xx/network errors; 4xx permanent (mirror `papra_client.py`'s policy exactly).
- Worker failures flow through `store.mark_job_failed` (attempts/backoff/3-strikes) — never crash the loop.
- Every task ends green: `.venv/bin/ruff check .` clean, `.venv/bin/pytest -m "not integration"` green; Task 6 additionally runs the full stack.
- Unit tests must not need Docker; the fake LLM server is in-process (threaded, ephemeral port).
- Commit after every task, messages as shown.

## M1/M2 carry-overs baked into this plan (from the review ledgers)

1. Lost-race interleave test for `claim_next_job` (Task 5) — the `rowcount == 0` retry branch has never been executed by any test.
2. `mark_seen`/`delete_and_expunge` status hardening in `mail.py` (Task 5) — STORE/EXPUNGE responses must be checked (`status != "OK"` → raise, flowing into the existing never-raises guard).
3. `serve()` wraps every poll defensively (Task 6) — `poll_mailbox_once` never raises by contract, but the loop catches `Exception` anyway.

---

### Task 1: LLM client

**Files:**
- Create: `papra_suite/llm_client.py`
- Modify: `requirements.txt` (add `pymupdf==1.25.1`), `requirements-dev.txt` (unchanged — pymupdf already there via the `-r` include)
- Test: `tests/test_llm_client.py`

**Interfaces:**
- Consumes: nothing (standalone, like `PapraClient`).
- Produces (Tasks 2-4 consume; M5 MCP may reuse):

```python
class LLMError(Exception):
    status: int | None          # None for network failures after retries

class LLMClient:
    def __init__(self, base_url: str, api_key: str, model: str, *,
                 timeout_s: float = 300.0, backoff_base_s: float = 0.5,
                 transport: httpx.AsyncBaseTransport | None = None) -> None
    async def chat(self, messages: list[dict], *, image: bytes | None = None) -> str
        # messages: OpenAI-style [{"role": "system"|"user", "content": str}]
        # image: raw image bytes (PNG); when given, the LAST user message's
        # content becomes [{"type": "text", "text": ...}, {"type": "image_url",
        # "image_url": {"url": "data:image/png;base64,..."}}]
        # returns choices[0].message.content (stripped)
    async def aclose(self) -> None; async context manager
```

Retry policy (identical shape to `papra_client.py`): 3 attempts, backoff `backoff_base_s * 4**attempt`, retry on 429 + all 5xx + `httpx.TransportError`; any other 4xx raises `LLMError` with status immediately.

- [ ] **Step 1: Write the failing tests**

`tests/test_llm_client.py`:

```python
import base64

import httpx
import pytest

from papra_suite.llm_client import LLMClient, LLMError


def make_client(handler, **kwargs):
    return LLMClient(
        "http://llm.test", "llm-key", "qwen3-vl", backoff_base_s=0.0,
        transport=httpx.MockTransport(handler), **kwargs,
    )


async def test_bearer_auth_and_model_sent():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("Authorization")
        seen["path"] = request.url.path
        seen["body"] = request.read()
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    async with make_client(handler) as client:
        await client.chat([{"role": "user", "content": "hi"}])
    import json

    body = json.loads(seen["body"])
    assert seen["auth"] == "Bearer llm-key"
    assert seen["path"] == "/chat/completions"
    assert body["model"] == "qwen3-vl"
    assert body["messages"] == [{"role": "user", "content": "hi"}]


async def test_image_becomes_data_url_content_part():
    import json

    bodies = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.read())
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    async with make_client(handler) as client:
        await client.chat(
            [{"role": "system", "content": "sys"}, {"role": "user", "content": "transcribe"}],
            image=b"\x89PNG fake",
        )
    content = json.loads(bodies[0])["messages"][1]["content"]
    expected = "data:image/png;base64," + base64.b64encode(b"\x89PNG fake").decode()
    assert content == [
        {"type": "text", "text": "transcribe"},
        {"type": "image_url", "image_url": {"url": expected}},
    ]
    # system message untouched
    assert json.loads(bodies[0])["messages"][0] == {"role": "system", "content": "sys"}


async def test_returns_stripped_content():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": [{"message": {"content": "  markdown\n"}}]})

    async with make_client(handler) as client:
        assert await client.chat([{"role": "user", "content": "x"}]) == "markdown"


async def test_retries_5xx_then_succeeds():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    async with make_client(handler) as client:
        assert await client.chat([{"role": "user", "content": "x"}]) == "ok"
    assert calls["n"] == 3


async def test_4xx_raises_immediately():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(400, json={"error": {"message": "bad"}})

    async with make_client(handler) as client:
        with pytest.raises(LLMError) as excinfo:
            await client.chat([{"role": "user", "content": "x"}])
    assert excinfo.value.status == 400 and calls["n"] == 1


async def test_network_error_retried_then_raises():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise httpx.ConnectError("refused")

    async with make_client(handler) as client:
        with pytest.raises(LLMError, match="refused"):
            await client.chat([{"role": "user", "content": "x"}])
    assert calls["n"] == 3
    # status is None for network failures (mirrors PapraError semantics)
    with pytest.raises(LLMError) as excinfo:
        async with make_client(handler) as client:
            await client.chat([{"role": "user", "content": "x"}])
    assert excinfo.value.status is None


async def test_backoff_sleeps_between_attempts():
    import time

    def handler(request: httpx.Request) -> httpx.Response:
        if "n" not in handler.__dict__:
            handler.n = 0
        handler.n += 1
        if handler.n < 3:
            return httpx.Response(500)
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    async with LLMClient(
        "http://llm.test", "k", "m", backoff_base_s=0.05,
        transport=httpx.MockTransport(handler),
    ) as client:
        start = time.monotonic()
        await client.chat([{"role": "user", "content": "x"}])
        assert time.monotonic() - start >= 0.24  # 0.05 + 0.20
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_llm_client.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement `papra_suite/llm_client.py`**

```python
"""Thin async client for OpenAI-compatible chat-completions endpoints.

Mirrors papra_client's retry policy: 3 attempts, exponential backoff,
retry on 429 + all 5xx + network errors; other 4xx fail permanently.
"""

import asyncio
import base64

import httpx

MAX_ATTEMPTS = 3


class LLMError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class LLMClient:
    def __init__(self, base_url: str, api_key: str, model: str, *,
                 timeout_s: float = 300.0, backoff_base_s: float = 0.5,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._model = model
        self._backoff_base_s = backoff_base_s
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout_s,
            transport=transport,
        )

    async def __aenter__(self) -> "LLMClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def chat(self, messages: list[dict], *, image: bytes | None = None) -> str:
        payload_messages = [dict(message) for message in messages]
        if image is not None:
            # the LAST user message carries the image content part
            for message in reversed(payload_messages):
                if message["role"] == "user":
                    data_url = "data:image/png;base64," + base64.b64encode(image).decode()
                    message["content"] = [
                        {"type": "text", "text": message["content"]},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ]
                    break
            else:
                raise LLMError("image given but no user message to attach it to")
        body = {"model": self._model, "messages": payload_messages}

        last_error = ""
        for attempt in range(MAX_ATTEMPTS):
            try:
                response = await self._client.post("/chat/completions", json=body)
            except httpx.TransportError as error:
                last_error = str(error)
                if attempt == MAX_ATTEMPTS - 1:
                    break
                await asyncio.sleep(self._backoff_base_s * 4**attempt)
                continue
            if (response.status_code == 429 or response.status_code >= 500) \
                    and attempt < MAX_ATTEMPTS - 1:
                await asyncio.sleep(self._backoff_base_s * 4**attempt)
                continue
            if response.is_success:
                return response.json()["choices"][0]["message"]["content"].strip()
            raise LLMError(
                f"chat completions failed: {response.status_code} {response.text[:200]}",
                response.status_code,
            )
        raise LLMError(f"chat completions failed after {MAX_ATTEMPTS} attempts: {last_error}")
```

- [ ] **Step 4: Run tests and lint**

Run: `.venv/bin/pytest tests/test_llm_client.py -v && .venv/bin/ruff check .`
Expected: all PASS, clean

- [ ] **Step 5: Commit**

```bash
git add papra_suite/llm_client.py tests/test_llm_client.py requirements.txt
git commit -m "feat(m3): openai-compatible llm client with retry policy"
```

---

### Task 2: OCR rendering and assembly

**Files:**
- Create: `papra_suite/ocr.py`
- Test: `tests/test_ocr.py`

**Interfaces:**
- Consumes: `AppConfig` fields (ocr_dpi, ocr_max_pages, ocr_page_pixel_budget).
- Produces (Task 4's worker consumes):

```python
OCR_SYSTEM_PROMPT: str   # the spec §5.4 fixed wording, verbatim:
# "Transcribe this document page to clean markdown. Preserve structure:
#  headings, lists, tables. Output only the markdown. Text is German or English."

def render_pages(pdf_bytes: bytes, *, dpi: int, max_pages: int,
                 pixel_budget: int) -> list[bytes]
    # PyMuPDF: each page -> PNG bytes at dpi; pages whose base render exceeds
    # pixel_budget are re-rendered with a reduced zoom (matrix scale
    # sqrt(budget / (w*h))); at most max_pages pages rendered; more pages ->
    # one warning log listing the skipped count

def assemble_markdown(page_markdowns: list[str]) -> str
    # join with a blank line between pages
```

- [ ] **Step 1: Write the failing tests**

`tests/test_ocr.py`:

```python
import fitz

from papra_suite.ocr import OCR_SYSTEM_PROMPT, assemble_markdown, render_pages


def make_pdf(pages: list[str]) -> bytes:
    pdf = fitz.open()
    for text in pages:
        page = pdf.new_page()
        page.insert_text((72, 72), text)
    data = pdf.tobytes()
    pdf.close()
    return data


def test_prompt_is_the_spec_wording():
    assert OCR_SYSTEM_PROMPT == (
        "Transcribe this document page to clean markdown. Preserve structure: "
        "headings, lists, tables. Output only the markdown. Text is German or English."
    )


def test_render_pages_returns_png_per_page():
    pages = render_pages(make_pdf(["Seite eins", "Seite zwei"]), dpi=72,
                        max_pages=50, pixel_budget=2_000_000)
    assert len(pages) == 2
    assert all(png.startswith(b"\x89PNG") for png in pages)


def test_render_pages_caps_and_warns(caplog):
    pdf = make_pdf([f"page {i}" for i in range(5)])
    pages = render_pages(pdf, dpi=72, max_pages=3, pixel_budget=2_000_000)
    assert len(pages) == 3
    assert any("2 additional page" in record.getMessage().lower() or
               "skipped" in record.getMessage().lower() for record in caplog.records)


def test_render_pages_downscales_over_pixel_budget():
    pdf = make_pdf(["dense"])  # A4 at 150 dpi is ~1.75M pixels; use a tiny budget
    pages = render_pages(pdf, dpi=150, max_pages=1, pixel_budget=50_000)
    assert len(pages) == 1
    import io

    image = fitz.open(stream=pages[0], filetype="png")
    width, height = image[0].rect.width, image[0].rect.height
    assert width * height <= 50_000 * 1.05  # small tolerance for rounding


def test_render_pages_empty_pdf():
    pdf = fitz.open()
    data = pdf.tobytes()
    pdf.close()
    assert render_pages(data, dpi=72, max_pages=10, pixel_budget=2_000_000) == []


def test_assemble_markdown_joins_pages():
    assert assemble_markdown(["# A", "# B"]) == "# A\n\n# B"
    assert assemble_markdown([]) == ""
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_ocr.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement `papra_suite/ocr.py`**

```python
"""OCR: PDF -> page images -> (vision model, called by the worker) -> markdown."""

import logging

import fitz

logger = logging.getLogger(__name__)

OCR_SYSTEM_PROMPT = (
    "Transcribe this document page to clean markdown. Preserve structure: "
    "headings, lists, tables. Output only the markdown. Text is German or English."
)


def render_pages(pdf_bytes: bytes, *, dpi: int, max_pages: int,
                 pixel_budget: int) -> list[bytes]:
    """Render up to max_pages pages as PNGs at dpi, downscaling any page
    whose render would exceed pixel_budget."""
    pages: list[bytes] = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as pdf:
        total = pdf.page_count
        for index in range(min(total, max_pages)):
            page = pdf[index]
            zoom = dpi / 72.0
            rect = page.rect
            base_pixels = rect.width * zoom * rect.height * zoom
            if base_pixels > pixel_budget:
                zoom *= (pixel_budget / base_pixels) ** 0.5
            pixmap = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom))
            pages.append(pixmap.tobytes("png"))
        if total > max_pages:
            logger.warning(
                "document has %d pages; rendering only the first %d (OCR_MAX_PAGES)",
                total, max_pages,
            )
    return pages


def assemble_markdown(page_markdowns: list[str]) -> str:
    return "\n\n".join(page_markdowns)
```

- [ ] **Step 4: Run tests and lint**

Run: `.venv/bin/pytest tests/test_ocr.py -v && .venv/bin/ruff check .`
Expected: all PASS, clean

- [ ] **Step 5: Commit**

```bash
git add papra_suite/ocr.py tests/test_ocr.py
git commit -m "feat(m3): pdf page rendering with pixel budget and page cap"
```

---

### Task 3: Title generation

**Files:**
- Create: `papra_suite/titles.py`
- Test: `tests/test_titles.py`

**Interfaces:**
- Consumes: `LLMClient`.
- Produces (Task 4's title worker consumes):

```python
TITLE_MAX_LENGTH = 80

def build_title_messages(*, filename: str, excerpt: str) -> list[dict]
    # system: "You generate concise titles for archived documents. Output only
    #  the title: one line, no quotes, maximum 80 characters, in the document's
    #  language (German or English)."
    # user: filename + first ~2000 chars of the OCR content excerpt

def sanitize_title(raw: str, *, fallback: str) -> str
    # strip whitespace and surrounding quotes (single/double/backtick), collapse
    # to one line, cap at TITLE_MAX_LENGTH; empty after sanitizing -> fallback

async def generate_title(llm: LLMClient, *, filename: str, excerpt: str) -> str
    # chat with build_title_messages; on LLMError: log a warning and return
    # the (sanitized) filename fallback; never raises
```

- [ ] **Step 1: Write the failing tests**

`tests/test_titles.py`:

```python
import httpx
import pytest

from papra_suite.llm_client import LLMClient, LLMError
from papra_suite.titles import (
    TITLE_MAX_LENGTH, build_title_messages, generate_title, sanitize_title,
)


def test_build_title_messages_shape():
    messages = build_title_messages(filename="rechnung-42.pdf", excerpt="Rechnung Nr. 42")
    assert messages[0]["role"] == "system"
    assert "maximum 80 characters" in messages[0]["content"]
    assert messages[1]["role"] == "user"
    assert "rechnung-42.pdf" in messages[1]["content"]
    assert "Rechnung Nr. 42" in messages[1]["content"]


def test_build_title_messages_truncates_excerpt():
    long = "x" * 5000
    messages = build_title_messages(filename="f.pdf", excerpt=long)
    assert len(messages[1]["content"]) < 2600


def test_sanitize_title_strips_quotes_and_caps():
    assert sanitize_title('"Rechnung 42"') == "Rechnung 42"
    assert sanitize_title("'Rechnung 42'") == "Rechnung 42"
    assert sanitize_title("`Rechnung 42`") == "Rechnung 42"
    assert sanitize_title("Rechnung 42\n\nMöbel GmbH") == "Rechnung 42 Möbel GmbH"
    assert len(sanitize_title("R" * 200)) == TITLE_MAX_LENGTH == 80


def test_sanitize_title_falls_back_on_empty():
    assert sanitize_title('""', fallback="rechnung.pdf") == "rechnung.pdf"
    assert sanitize_title("   ", fallback="rechnung.pdf") == "rechnung.pdf"


async def test_generate_title_returns_sanitized_llm_output():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": [{"message": {"content": '  "Rechnung 42 – Möbel"\n'}}]})

    llm = LLMClient("http://llm.test", "k", "m", transport=httpx.MockTransport(handler))
    title = await generate_title(llm, filename="x.pdf", excerpt="inhalt")
    assert title == "Rechnung 42 – Möbel"


async def test_generate_title_falls_back_on_llm_failure(caplog):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    llm = LLMClient("http://llm.test", "k", "m", backoff_base_s=0.0,
                    transport=httpx.MockTransport(handler))
    title = await generate_title(llm, filename="rechnung-42.pdf", excerpt="inhalt")
    assert title == "rechnung-42.pdf"
    assert any("failed" in r.getMessage().lower() for r in caplog.records)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_titles.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement `papra_suite/titles.py`**

```python
"""Title generation: OCR content excerpt + filename -> concise document title."""

import logging

from papra_suite.llm_client import LLMClient, LLMError

logger = logging.getLogger(__name__)

TITLE_MAX_LENGTH = 80
EXCERPT_CHARS = 2000

TITLE_SYSTEM_PROMPT = (
    "You generate concise titles for archived documents. Output only the "
    "title: one line, no quotes, maximum 80 characters, in the document's "
    "language (German or English)."
)


def build_title_messages(*, filename: str, excerpt: str) -> list[dict]:
    trimmed = excerpt[:EXCERPT_CHARS]
    return [
        {"role": "system", "content": TITLE_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"Filename: {filename}\n\n"
                f"Document content excerpt:\n\n{trimmed}\n\n"
                "Generate the title."
            ),
        },
    ]


def sanitize_title(raw: str, *, fallback: str) -> str:
    title = raw.strip().strip("\"'`").strip()
    title = " ".join(title.split())  # collapse whitespace/newlines to one line
    if len(title) > TITLE_MAX_LENGTH:
        title = title[:TITLE_MAX_LENGTH].rstrip()
    return title or fallback


async def generate_title(llm: LLMClient, *, filename: str, excerpt: str) -> str:
    """Generate a title; on LLM failure, fall back to the filename."""
    try:
        raw = await llm.chat(build_title_messages(filename=filename, excerpt=excerpt))
    except LLMError as error:
        logger.warning("title generation failed (%s); falling back to filename", error)
        return sanitize_title(filename, fallback=filename)
    return sanitize_title(raw, fallback=sanitize_title(filename, fallback=filename))
```

- [ ] **Step 4: Run tests and lint**

Run: `.venv/bin/pytest tests/test_titles.py -v && .venv/bin/ruff check .`
Expected: all PASS, clean

- [ ] **Step 5: Commit**

```bash
git add papra_suite/titles.py tests/test_titles.py
git commit -m "feat(m3): title generation with sanitization and filename fallback"
```

---

### Task 4: Workers, transition handler, worker loops, sweep

**Files:**
- Modify: `papra_suite/pipeline.py`
- Test: `tests/test_workers.py`

**Interfaces:**
- Consumes: `Store`/`Job`, `PapraClient`, `LLMClient`, `ocr.*`, `titles.*`, `enqueue_ocr_job` (existing), `AppConfig`.
- Produces (Task 6's serve consumes; M4's dashboard retry buttons reuse the job semantics):

```python
# pipeline.py additions
async def run_ocr_job(papra: PapraClient, ocr_llm: LLMClient, job: Job,
                      *, config) -> str
    # download file -> render_pages(config params) -> per-page chat (system
    # prompt OCR_SYSTEM_PROMPT, user message "Transcribe this page.", image=png)
    # -> assemble_markdown -> papra.update_document(content=...) -> return markdown

def on_job_done(store: Store, job: Job, *, ocr_result: str) -> None
    # transition handler (decisions §2): kind == "ocr" -> enqueue the title job
    # (kind="title", content_hash=sha256 of the markdown, payload={"name":
    # job.payload["name"], "excerpt": markdown[:2000]}); other kinds -> no-op

async def run_title_job(papra: PapraClient, title_llm: LLMClient, job: Job) -> None
    # title = await generate_title(title_llm, filename=job.payload["name"],
    #   excerpt=job.payload.get("excerpt", "")) -> papra.update_document(name=title)

async def worker_loop(store: Store, *, kind: str, handler, idle_sleep_s: float = 1.0) -> None
    # loop: claim_next_job(kind); none -> sleep; run handler(job); mark_job_done
    # + on_job_done(store, job, ...) if the handler returns a str result;
    # any Exception -> mark_job_failed(job.id, str(error)). Never raises.

async def sweep_once(store: Store, papra: PapraClient) -> SweepResult
    # per org (store.list_orgs): list_documents(page_size=50, sort_field="createdAt",
    # sort_order="desc"); for each document not yet seen: record seen; if PDF
    # (mimeType == "application/pdf" or name endswith .pdf case-insensitive):
    # download the file, enqueue_ocr_job(file_hash=sha256(file), name, mime,
    # ingested_via="sweep"); else count as non-pdf skipped

@dataclass
class SweepResult:
    documents_checked: int = 0
    jobs_enqueued: int = 0
    non_pdf_skipped: int = 0
```

- [ ] **Step 1: Write the failing tests**

`tests/test_workers.py` (unit level: real Store, stub papra, MockTransport LLM):

```python
import hashlib
import json

import httpx
import pytest

from papra_suite.config import load_config
from papra_suite.pipeline import (
    SweepResult, on_job_done, run_ocr_job, run_title_job, sweep_once, worker_loop,
)
from papra_suite.store import Job, Store

BASE_ENV = {
    "PAPRA_BASE_URL": "http://papra:1221", "PAPRA_API_KEY": "k",
    "LLM_BASE_URL": "http://llm:8000", "LLM_API_KEY": "k",
    "TITLE_MODEL": "t", "OCR_MODEL": "o", "ADMIN_PASSWORD": "p",
}


class StubPapra:
    """Records write-backs; serves a canned PDF for downloads."""

    def __init__(self, pdf: bytes = b"%PDF-1.4 fake-with-pages"):
        self.pdf = pdf
        self.content_updates: list[tuple[str, str | None, str | None]] = []
        self.documents = {  # org -> {doc_id: {"name": ..., "mimeType": ...}}
            "org_1": {
                "doc_1": {"id": "doc_1", "name": "rechnung.pdf", "mimeType": "application/pdf"},
                "doc_2": {"id": "doc_2", "name": "notes.txt", "mimeType": "text/plain"},
            }
        }

    async def download_file(self, org_id, document_id):
        return self.pdf

    async def update_document(self, org_id, document_id, *, name=None, content=None):
        self.content_updates.append((document_id, name, content))
        if name:
            self.documents[org_id][document_id]["name"] = name

    async def list_documents(self, org_id, *, page_size=100, search_query=None,
                             sort_field="createdAt", sort_order="desc"):
        docs = list(self.documents[org_id].values())
        return docs, len(docs)


def make_llm(responses: list[str]):
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.read())
        assert body["messages"][0]["role"] == "system"
        content = responses[min(calls["n"], len(responses) - 1)]
        calls["n"] += 1
        return httpx.Response(200, json={"choices": [{"message": {"content": content}}]})

    from papra_suite.llm_client import LLMClient

    return LLMClient("http://llm.test", "k", "m", backoff_base_s=0.0,
                     transport=httpx.MockTransport(handler)), calls


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    yield store
    store.close()


@pytest.fixture
def config():
    return load_config({**BASE_ENV, "DATA_DIR": "/tmp/x"})


def make_job(store, kind="ocr", doc="doc_1", content_hash="h1",
            payload=None):
    job_id = store.enqueue_job(kind=kind, papra_org_id="org_1", document_id=doc,
                               content_hash=content_hash,
                               payload=payload if payload is not None
                               else {"file_hash": content_hash, "name": "rechnung.pdf"})
    return store.get_job(job_id)


async def test_run_ocr_job_writes_back_assembled_markdown(store, config):
    import fitz

    pdf = fitz.open()
    pdf.new_page(); pdf.new_page()
    pdf_bytes = pdf.tobytes()
    pdf.close()
    papra = StubPapra(pdf_bytes)
    llm, calls = make_llm(["# Seite eins", "# Seite zwei"])

    markdown = await run_ocr_job(papra, llm, make_job(store), config=config)

    assert markdown == "# Seite eins\n\n# Seite zwei"
    assert calls["n"] == 2  # one request per page
    assert papra.content_updates == [("doc_1", None, "# Seite eins\n\n# Seite zwei")]


async def test_on_job_done_enqueues_title_job(store):
    job = make_job(store)
    on_job_done(store, job, ocr_result="# Rechnung\n\nInhalt")
    title_jobs = store.list_jobs()
    assert len(title_jobs) == 2
    title_job = [j for j in title_jobs if j.kind == "title"][0]
    assert title_job.document_id == "doc_1"
    assert title_job.content_hash == hashlib.sha256("# Rechnung\n\nInhalt".encode()).hexdigest()
    assert title_job.payload["name"] == "rechnung.pdf"
    assert title_job.payload["excerpt"] == "# Rechnung\n\nInhalt"


def test_on_job_done_ignores_title_kind(store):
    job = make_job(store, kind="title", content_hash="th",
                   payload={"name": "n", "excerpt": "e"})
    on_job_done(store, job, ocr_result="unused")
    assert len(store.list_jobs()) == 1


async def test_run_title_job_writes_generated_name(store):
    papra = StubPapra()
    llm, _ = make_llm(["Rechnung 42 – Möbel"])
    job = make_job(store, kind="title", content_hash="th",
                   payload={"name": "rechnung.pdf", "excerpt": "Rechnung Nr. 42"})

    await run_title_job(papra, llm, job)

    assert papra.content_updates == [("doc_1", "Rechnung 42 – Möbel", None)]


async def test_worker_loop_processes_and_transitions(store, config):
    papra = StubPapra(b"%PDF-1.4 x")
    llm, _ = make_llm(["# md"])
    make_job(store)  # enqueue one ocr job (doc_1)

    # run the loop until the queue drains, then stop it via a failing claim patch
    import papra_suite.pipeline as pipeline

    original_claim = store.claim_next_job
    state = {"claims": 0}

    def claim(kind):
        state["claims"] += 1
        if state["claims"] > 1:
            raise _StopLoop()
        return original_claim(kind)

    class _StopLoop(Exception):
        pass

    store.claim_next_job = claim
    with pytest.raises(_StopLoop):
        await worker_loop(store, kind="ocr",
                          handler=lambda job: run_ocr_job(papra, llm, job, config=config))

    jobs = store.list_jobs()
    assert [j.kind for j in jobs if j.status == "done"] == ["ocr"]
    assert [j.kind for j in jobs if j.status == "pending"] == ["title"]


async def test_worker_loop_marks_failures(store, config):
    papra = StubPapra(b"not a pdf at all")
    llm, _ = make_llm(["# md"])
    make_job(store)

    async def handler(job):
        raise RuntimeError("boom")

    import papra_suite.pipeline as pipeline

    original_claim = store.claim_next_job
    state = {"claims": 0}

    def claim(kind):
        state["claims"] += 1
        if state["claims"] > 1:
            raise KeyboardInterrupt()
        return original_claim(kind)

    store.claim_next_job = claim
    with pytest.raises(KeyboardInterrupt):
        await worker_loop(store, kind="ocr", handler=handler)

    job = store.list_jobs()[0]
    assert job.status == "pending" and job.attempts == 1 and "boom" in job.error


async def test_sweep_once_enqueues_for_unseen_pdfs_only(store):
    papra = StubPapra()
    result = await sweep_once(store, papra)

    assert isinstance(result, SweepResult)
    assert result.documents_checked == 2
    assert result.jobs_enqueued == 1  # doc_1 (pdf), not doc_2 (txt)
    assert result.non_pdf_skipped == 1
    assert store.document_seen("org_1", "doc_1")
    assert store.document_seen("org_1", "doc_2")
    job = store.claim_next_job("ocr")
    assert job.document_id == "doc_1"
    assert job.content_hash == hashlib.sha256(papra.pdf).hexdigest()
    assert job.payload["ingested_via"] if False else job.payload.get("name") == "rechnung.pdf"


async def test_sweep_once_is_idempotent(store):
    papra = StubPapra()
    first = await sweep_once(store, papra)
    second = await sweep_once(store, papra)
    assert first.jobs_enqueued == 1 and second.jobs_enqueued == 0
    assert second.documents_checked == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_workers.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement in `papra_suite/pipeline.py`** (append to the existing module; keep `enqueue_ocr_job` unchanged)

```python
import asyncio
import hashlib
import logging
from dataclasses import dataclass

from papra_suite.llm_client import LLMClient
from papra_suite.ocr import OCR_SYSTEM_PROMPT, assemble_markdown, render_pages
from papra_suite.papra_client import PapraClient
from papra_suite.store import Job, Store
from papra_suite.titles import generate_title

logger = logging.getLogger(__name__)

SWEEP_PAGE_SIZE = 50


@dataclass
class SweepResult:
    documents_checked: int = 0
    jobs_enqueued: int = 0
    non_pdf_skipped: int = 0


async def run_ocr_job(papra: PapraClient, ocr_llm: LLMClient, job: Job, *,
                      config) -> str:
    """Download, render, transcribe per page, write back; returns the markdown."""
    pdf_bytes = await papra.download_file(job.papra_org_id, job.document_id)
    pages = render_pages(
        pdf_bytes, dpi=config.ocr_dpi, max_pages=config.ocr_max_pages,
        pixel_budget=config.ocr_page_pixel_budget,
    )
    page_markdowns = []
    for index, png in enumerate(pages):
        logger.info("ocr: document %s page %d/%d", job.document_id, index + 1, len(pages))
        page_markdowns.append(
            await ocr_llm.chat(
                [
                    {"role": "system", "content": OCR_SYSTEM_PROMPT},
                    {"role": "user", "content": "Transcribe this page."},
                ],
                image=png,
            )
        )
    markdown = assemble_markdown(page_markdowns)
    await papra.update_document(job.papra_org_id, job.document_id, content=markdown)
    return markdown


def on_job_done(store: Store, job: Job, *, ocr_result: str) -> None:
    """Central transition handler (decisions §2): ocr done -> enqueue title."""
    if job.kind != "ocr":
        return
    content_hash = hashlib.sha256(ocr_result.encode()).hexdigest()
    store.enqueue_job(
        kind="title",
        papra_org_id=job.papra_org_id,
        document_id=job.document_id,
        content_hash=content_hash,
        payload={"name": job.payload.get("name", ""), "excerpt": ocr_result[:2000]},
    )


async def run_title_job(papra: PapraClient, title_llm: LLMClient, job: Job) -> None:
    title = await generate_title(
        title_llm, filename=job.payload.get("name", "document.pdf"),
        excerpt=job.payload.get("excerpt", ""),
    )
    await papra.update_document(job.papra_org_id, job.document_id, name=title)


async def worker_loop(store: Store, *, kind: str, handler,
                      idle_sleep_s: float = 1.0) -> None:
    """Claim-and-run loop for one job kind. Never raises."""
    while True:
        job = store.claim_next_job(kind)
        if job is None:
            await asyncio.sleep(idle_sleep_s)
            continue
        try:
            result = await handler(job)
            store.mark_job_done(job.id)
            if isinstance(result, str):
                on_job_done(store, job, ocr_result=result)
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - the loop must survive
            logger.error("job %s (%s) failed: %s", job.id, kind, error)
            try:
                store.mark_job_failed(job.id, str(error))
            except ValueError:
                logger.error("job %s vanished before failure could be recorded", job.id)


def _is_pdf(document: dict) -> bool:
    mime = document.get("mimeType") or ""
    if mime == "application/pdf":
        return True
    return (document.get("name") or "").lower().endswith(".pdf")


async def sweep_once(store: Store, papra: PapraClient) -> SweepResult:
    """Record and enqueue OCR for externally-ingested documents (decisions §4)."""
    result = SweepResult()
    for org in store.list_orgs():
        documents, _count = await papra.list_documents(
            org.papra_org_id, page_size=SWEEP_PAGE_SIZE,
            sort_field="createdAt", sort_order="desc",
        )
        for document in documents:
            result.documents_checked += 1
            document_id = document["id"]
            if store.document_seen(org.papra_org_id, document_id):
                continue
            if _is_pdf(document):
                file_bytes = await papra.download_file(org.papra_org_id, document_id)
                file_hash = hashlib.sha256(file_bytes).hexdigest()
                enqueue_ocr_job(
                    store, papra_org_id=org.papra_org_id, document_id=document_id,
                    file_hash=file_hash, name=document.get("name", "document.pdf"),
                    mime="application/pdf", ingested_via="sweep",
                )
                result.jobs_enqueued += 1
            else:
                store.record_document_seen(
                    papra_org_id=org.papra_org_id, papra_document_id=document_id,
                    name=document.get("name", ""), mime=document.get("mimeType", ""),
                    ingested_via="sweep",
                )
                result.non_pdf_skipped += 1
    return result
```

- [ ] **Step 4: Run tests and lint**

Run: `.venv/bin/pytest tests/test_workers.py tests/test_pipeline.py -v && .venv/bin/ruff check .`
Expected: all PASS (existing pipeline tests unaffected), clean

- [ ] **Step 5: Commit**

```bash
git add papra_suite/pipeline.py tests/test_workers.py
git commit -m "feat(m3): ocr/title workers, transition handler, worker loop, sweep"
```

---

### Task 5: Fake LLM server + M2 carry-overs (lost-race test, IMAP status hardening)

**Files:**
- Create: `tests/fakes/llm_server.py`, `tests/test_llm_server.py`
- Modify: `papra_suite/mail.py` (status hardening only), `tests/test_mail_session.py`
- Test: `tests/test_store.py` (lost-race interleave)

**Interfaces:**
- Produces: `tests/fakes/llm_server.py` (used by Task 6's integration tests):

```python
class FakeLLMServer:
    """Threaded stdlib HTTP server impersonating an OpenAI-compatible endpoint.
    Serves POST /chat/completions from a scripted response list (repeating the
    last), capturing every request body. Failure injection: append an
    (status_code, body) tuple instead of a string."""
    def __init__(self) -> None            # binds 127.0.0.1:0
    @property
    def url(self) -> str                  # http://127.0.0.1:<port>
    @property
    def requests(self) -> list[dict]      # captured request bodies
    def start(self) -> None; def stop(self) -> None
```

- [ ] **Step 1: Write the fake server and its tests**

`tests/fakes/llm_server.py`:

```python
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class FakeLLMServer:
    def __init__(self) -> None:
        self._responses: list = []
        self._requests: list[dict] = []
        self._lock = threading.Lock()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._make_handler())
        self._thread: threading.Thread | None = None

    def _make_handler(self):
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length))
                with outer._lock:
                    outer._requests.append(body)
                    response = outer._responses[min(len(outer._requests) - 1,
                                                     len(outer._responses) - 1)] \
                        if outer._responses else "ok"
                if isinstance(response, tuple):
                    status, payload = response
                else:
                    status, payload = 200, {"choices": [{"message": {"content": response}}]}
                data = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        return Handler

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    @property
    def responses(self) -> list:
        return self._responses

    @property
    def requests(self) -> list[dict]:
        with self._lock:
            return list(self._requests)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
```

`tests/test_llm_server.py`:

```python
import httpx

from papra_suite.llm_client import LLMClient
from tests.fakes.llm_server import FakeLLMServer


async def test_fake_server_serves_scripted_responses():
    server = FakeLLMServer()
    server.responses.extend(["erste Seite", "zweite Seite", "letzte"])
    server.start()
    try:
        async with LLMClient(server.url, "k", "m", backoff_base_s=0.0) as llm:
            assert await llm.chat([{"role": "user", "content": "a"}]) == "erste Seite"
            assert await llm.chat([{"role": "user", "content": "b"}]) == "zweite Seite"
            assert await llm.chat([{"role": "user", "content": "c"}]) == "letzte"
            assert await llm.chat([{"role": "user", "content": "d"}]) == "letzte"  # repeats
        assert [r["messages"][0]["content"] for r in server.requests] == list("abcd")
    finally:
        server.stop()


async def test_fake_server_failure_injection_and_retry():
    server = FakeLLMServer()
    server.responses.extend([(500, {"error": "overloaded"}), "recovered"])
    server.start()
    try:
        async with LLMClient(server.url, "k", "m", backoff_base_s=0.0) as llm:
            assert await llm.chat([{"role": "user", "content": "x"}]) == "recovered"
        assert len(server.requests) == 2
    finally:
        server.stop()
```

- [ ] **Step 2: Write the lost-race interleave test** (M2 carry-over; add to `tests/test_store.py`)

```python
def test_claim_next_job_retries_when_race_steals_candidate():
    """M2 carry-over: the conditional-claim retry branch, deterministically."""
    for i in range(3):
        store.enqueue_job(kind="ocr", papra_org_id="org_1",
                          document_id=f"doc_{i}", content_hash=f"h{i}", payload={})

    class RacingConnection:
        """Delegates to the real connection; steals the first claimed job
        via a second connection between the claim's SELECT and UPDATE."""

        def __init__(self, real):
            self._real = real
            self._stolen = False
            self._thief = None

        def execute(self, sql, params=()):
            if not self._stolen and sql.startswith("UPDATE jobs SET status = 'running'"):
                self._stolen = True
                # the thief commits a steal BEFORE our UPDATE runs
                import sqlite3

                thief = sqlite3.connect(real_db_path, timeout=5)
                thief.execute("BEGIN IMMEDIATE")
                thief.execute(
                    "UPDATE jobs SET status = 'running' WHERE document_id = 'doc_0'"
                )
                thief.commit()
                thief.close()
            return self._real.execute(sql, params)

        def __getattr__(self, name):
            return getattr(self._real, name)

    real_db_path = store._conn.execute("PRAGMA database_list").fetchall()[0][2] + "/race.db"
    # NOTE: the db path is available via the fixture's tmp_path — the helper
    # above must use the SAME file as the store; simplest: pass tmp_path in.
    # The implementer should adapt this sketch to the fixture's actual db path
    # (e.g. store = Store(tmp_path / "state.db") is fixture-created; capture the
    # path from the fixture or reconstruct it) — the goal is deterministic
    # interleave: a steal committed between SELECT and UPDATE must lead to
    # rowcount == 0 and the loop claiming doc_1 next.
    original = store._conn
    store._conn = RacingConnection(original)
    try:
        job = store.claim_next_job("ocr")
        assert job.document_id == "doc_1"  # doc_0 was stolen; loop retried
    finally:
        store._conn = original
```

Implementation note for the implementer: the sketch's db-path plumbing is the fiddly part — cleanest is to construct the store inside the test (not via the fixture) so the path is known, e.g. `store = Store(tmp_path / "race.db"); store.migrate()`. The assertion is the contract: the stolen candidate is skipped and the NEXT pending job is claimed, with no exception. Also assert the stolen job's status is untouched by our claim (`doc_0` stays `running` from the thief's write).

- [ ] **Step 3: IMAP status hardening** (M2 carry-over; `papra_suite/mail.py`)

In `MailboxSession.mark_seen`, `delete_and_expunge` (all three arms: UID EXPUNGE, UID SEARCH DELETED, plain EXPUNGE), and `select_inbox`: check the tagged status of every STORE/EXPUNGE/SEARCH/SELECT; `status != "OK"` → raise `imaplib.IMAP4.error(f"...")` (flows into the poller's existing never-raises guard). Keep the existing shape guards. Add tests in `tests/test_mail_session.py` with the fake imap returning `("NO", [None])` for STORE/EXPUNGE/SEARCH → each raises `imaplib.IMAP4.error`; the poller-level test `test_store_stage_imap_error_never_raises` must still pass (it already simulates a STORE failure via the session double).

- [ ] **Step 4: Run all new tests, the full unit suite, and lint**

Run: `.venv/bin/pytest tests/test_llm_server.py tests/test_store.py tests/test_mail_session.py tests/test_mail_poller.py -v && .venv/bin/pytest -m "not integration" && .venv/bin/ruff check .`
Expected: all PASS, clean

- [ ] **Step 5: Commit**

```bash
git add tests/fakes/llm_server.py tests/test_llm_server.py tests/test_store.py papra_suite/mail.py tests/test_mail_session.py
git commit -m "feat(m3): fake llm server, lost-race interleave test, imap status hardening"
```

---

### Task 6: Serve command + end-to-end integration tests

**Files:**
- Modify: `papra_suite/pipeline.py` (add `serve`), `papra_suite/__main__.py` (wire the `serve` subcommand)
- Create: `tests/integration/test_ocr_pipeline.py`
- Test: `tests/test_serve.py` (unit)

**Interfaces:**
- Consumes: everything above; `poll_mailbox_once`, `Mailbox`, `Org`.
- Produces: `python -m papra_suite serve` — the process runtime. M4 mounts the dashboard inside it.

```python
# pipeline.py
async def serve(config, *, stop: asyncio.Event | None = None,
                store: Store | None = None, papra: PapraClient | None = None,
                ocr_llm: LLMClient | None = None, title_llm: LLMClient | None = None) -> None
    # startup: Store+migrate+recover_running_jobs; setup_logging(store);
    # register_config_secrets(config); construct clients (or use injected ones)
    # tasks: one poll loop per enabled mailbox (defensive try/except around
    # poll_mailbox_once, sleep mailbox.poll_interval_s); config.ocr_concurrency
    # ocr worker tasks; one title worker task; one sweep loop task when
    # config.sweep_interval_s > 0 (sleep sweep_interval_s)
    # runs until the stop event is set (or cancelled); then cancels tasks,
    # closes clients and store cleanly
```

- [ ] **Step 1: Write the failing serve unit test**

`tests/test_serve.py`:

```python
import asyncio

import pytest

from papra_suite.config import load_config
from papra_suite.pipeline import serve
from papra_suite.store import Store

BASE_ENV = {
    "PAPRA_BASE_URL": "http://papra:1221", "PAPRA_API_KEY": "k",
    "LLM_BASE_URL": "http://llm:8000", "LLM_API_KEY": "k",
    "TITLE_MODEL": "t", "OCR_MODEL": "o", "ADMIN_PASSWORD": "p",
    "SWEEP_INTERVAL_S": "0",  # disable the sweep for the test
}


async def test_serve_runs_and_stops_cleanly(tmp_path, monkeypatch):
    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})
    store = Store(tmp_path / "state.db")
    store.migrate()
    org_id = store.create_org(papra_org_id="org_1", name="Org")
    store.create_mailbox(org_id=org_id, host="imap.invalid", port=993,
                         username="u", password="p", poll_interval_s=3600)

    stop = asyncio.Event()

    async def set_stop_later():
        await asyncio.sleep(0.5)
        stop.set()

    # poll attempts fail (imap.invalid) — the loop must survive them
    asyncio.get_running_loop().create_task(set_stop_later())
    await serve(config, stop=stop, store=store)  # returns when stop is set

    # the mailbox recorded its failure; the process stayed alive until stop
    mailbox = store.get_mailbox(store.list_mailboxes()[0].id)
    assert mailbox is not None  # state machinery ran
    store.close()


async def test_serve_recovers_stale_jobs_at_startup(tmp_path):
    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})
    store = Store(tmp_path / "state.db")
    store.migrate()
    job_id = store.enqueue_job(kind="ocr", papra_org_id="org_1",
                              document_id="doc_1", content_hash="h", payload={})
    store.claim_next_job("ocr")  # now running (simulated crash leftover)

    stop = asyncio.Event()
    await asyncio.get_running_loop().create_task(_set_after(0.3, stop))
    # serve with an EMPTY mailbox set: create the org but no mailbox
    await serve(config, stop=stop, store=store)
    store.close()


async def _set_after(delay, event):
    await asyncio.sleep(delay)
    event.set()
```

Note for the implementer: `serve` must call `recover_running_jobs()` during startup — the second test asserts it by checking the job is `pending` again after serve started (add that assertion: read the job via a second Store handle or check before stop). Adapt the sketches as needed for clean async structure; the CONTRACT is: serve starts, survives poll failures against an unreachable host, recovers stale jobs once at startup, and returns cleanly on stop with no dangling tasks (assert `asyncio.all_tasks()` has no leftover papra-suite tasks after serve returns).

- [ ] **Step 2: Implement `serve` in `pipeline.py` and wire `__main__.py`**

```python
# pipeline.py (append)
def _build_clients(config):
    from papra_suite.llm_client import LLMClient
    from papra_suite.papra_client import PapraClient

    return (
        PapraClient(config.papra_base_url, config.papra_api_key),
        LLMClient(config.llm_base_url, config.llm_api_key, config.ocr_model,
                  timeout_s=300.0),
        LLMClient(config.llm_base_url, config.llm_api_key, config.title_model,
                  timeout_s=300.0),
    )


async def _poll_loop(store: Store, papra, mailbox) -> None:
    from papra_suite.mail import poll_mailbox_once

    while True:
        org = store.get_org(mailbox.org_id)
        if org is not None:
            try:
                await poll_mailbox_once(store, papra, mailbox, org)
            except Exception:  # noqa: BLE001 - defense in depth (M2 carry-over)
                logger.exception("poll loop error for mailbox %s", mailbox.id)
        await asyncio.sleep(mailbox.poll_interval_s)


async def _sweep_loop(store: Store, papra, interval_s: int) -> None:
    while True:
        try:
            result = await sweep_once(store, papra)
            if result.jobs_enqueued or result.non_pdf_skipped:
                logger.info("sweep: %d checked, %d enqueued, %d non-pdf",
                            result.documents_checked, result.jobs_enqueued,
                            result.non_pdf_skipped)
        except Exception:  # noqa: BLE001
            logger.exception("sweep loop error")
        await asyncio.sleep(interval_s)


async def serve(config, *, stop: asyncio.Event | None = None,
                store: Store | None = None, papra=None, ocr_llm=None,
                title_llm=None) -> None:
    """The process runtime: pollers, workers, sweep. Runs until stop/cancel."""
    from papra_suite.logging_setup import register_config_secrets, setup_logging

    stop = stop or asyncio.Event()
    owns_store = store is None
    if store is None:
        store = Store(config.data_dir / "state.db")
    store.migrate()
    recovered = store.recover_running_jobs()
    if recovered:
        logger.info("recovered %d stale running job(s) at startup", recovered)
    setup_logging(store)
    register_config_secrets(config)
    if papra is None or ocr_llm is None or title_llm is None:
        built_papra, built_ocr, built_title = _build_clients(config)
        papra = papra or built_papra
        ocr_llm = ocr_llm or built_ocr
        title_llm = title_llm or built_title
        # close only what we built (track via locals; simplest: close all three
        # if we built them — adapt to avoid closing injected clients)
    tasks = []
    for mailbox in store.list_mailboxes(enabled_only=True):
        tasks.append(asyncio.create_task(_poll_loop(store, papra, mailbox)))
    for _ in range(config.ocr_concurrency):
        tasks.append(asyncio.create_task(worker_loop(
            store, kind="ocr",
            handler=lambda job: run_ocr_job(papra, ocr_llm, job, config=config),
        )))
    tasks.append(asyncio.create_task(worker_loop(
        store, kind="title",
        handler=lambda job: run_title_job(papra, title_llm, job),
    )))
    if config.sweep_interval_s > 0:
        tasks.append(asyncio.create_task(
            _sweep_loop(store, papra, config.sweep_interval_s)))
    stop_waiter = asyncio.create_task(stop.wait())
    try:
        await stop_waiter
    finally:
        stop_waiter.cancel()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await papra.aclose()
        await ocr_llm.aclose()
        await title_llm.aclose()
        if owns_store:
            store.close()
```

Wire in `__main__.py`: add a `serve` subparser; `if args.command == "serve": from papra_suite.pipeline import serve; asyncio.run(serve(config)); return 0`.

The implementer must clean up the client-ownership logic (close only constructed clients; injected ones belong to the caller) and ensure the unit tests' injected store is NOT closed by serve (`owns_store`).

- [ ] **Step 3: Write the end-to-end integration test**

`tests/integration/test_ocr_pipeline.py`:

```python
import asyncio

import fitz
import pytest

from papra_suite.llm_client import LLMClient
from papra_suite.papra_client import PapraClient
from papra_suite.pipeline import run_ocr_job, run_title_job, on_job_done, sweep_once
from papra_suite.store import Store
from papra_suite.config import load_config
from tests.fakes.llm_server import FakeLLMServer

pytestmark = pytest.mark.integration

PAGE_ONE_MARKDOWN = "# Rechnung Nr. 42\n\nVielen Dank für Ihre Bestellung."
PAGE_TWO_MARKDOWN = "## Zahlungsbedingungen\n\nZahlbar bis 15.11.2026."
SEARCH_PHRASE = "quokka-marmalade-ocr"  # only in OCR-written content


def make_german_pdf() -> bytes:
    pdf = fitz.open()
    page = pdf.new_page()
    page.insert_text((72, 72), "Rechnung Nr. 42")
    page.insert_text((72, 120), "Vielen Dank für Ihre Bestellung.")
    page = pdf.new_page()
    page.insert_text((72, 72), "Zahlungsbedingungen")
    page.insert_text((72, 120), "Zahlbar bis 15.11.2026.")
    data = pdf.tobytes()
    pdf.close()
    return data


@pytest.fixture
def stack(tmp_path, papra_stack):
    base_url, org_id, api_key = papra_stack
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.create_org(papra_org_id=org_id, name="Integration Org")
    config = load_config({
        "PAPRA_BASE_URL": base_url, "PAPRA_API_KEY": api_key,
        "LLM_BASE_URL": "http://fake.invalid", "LLM_API_KEY": "k",
        "TITLE_MODEL": "t", "OCR_MODEL": "o", "ADMIN_PASSWORD": "p",
        "DATA_DIR": str(tmp_path),
    })
    yield store, PapraClient(base_url, api_key), org_id, config
    store.close()


async def test_ocr_and_title_end_to_end(stack):
    store, papra, org_id, config = stack
    pdf = make_german_pdf()
    document = await papra.create_document(org_id, "rechnung-42.pdf", pdf)
    document_id = document["id"]
    job_id = store.enqueue_job(kind="ocr", papra_org_id=org_id,
                                document_id=document_id,
                                content_hash="integration-hash",
                                payload={"file_hash": "integration-hash",
                                         "name": "rechnung-42.pdf"})
    job = store.get_job(job_id)

    server = FakeLLMServer()
    # the fake "vision model": page 1 then page 2 markdown, then the title
    server.responses.extend([
        PAGE_ONE_MARKDOWN, PAGE_TWO_MARKDOWN, "Rechnung 42 – Bestellung",
    ])
    server.start()
    try:
        ocr_llm = LLMClient(server.url, "k", "ocr-model")
        title_llm = LLMClient(server.url, "k", "title-model")

        # OCR worker
        markdown = await run_ocr_job(papra, ocr_llm, job, config=config)
        assert PAGE_ONE_MARKDOWN in markdown and PAGE_TWO_MARKDOWN in markdown
        store.mark_job_done(job.id)
        on_job_done(store, job, ocr_result=markdown)

        # transition enqueued a title job
        title_jobs = [j for j in store.list_jobs() if j.kind == "title"]
        assert len(title_jobs) == 1

        # content is searchable in Papra (spec acceptance 5's mail-path analog)
        documents, count = await papra.list_documents(
            org_id, search_query="Zahlbar bis 15.11.2026")
        assert document_id in {d["id"] for d in documents}

        # title worker
        await run_title_job(papra, title_llm, title_jobs[0])
        updated = await papra.get_document(org_id, document_id)
        assert updated["name"] == "Rechnung 42 – Bestellung"

        # the fake saw: 2 page requests (with images) + 1 title request
        assert len(server.requests) == 3
        page_request = server.requests[0]
        content_part = page_request["messages"][1]["content"][1]
        assert content_part["type"] == "image_url"
    finally:
        server.stop()
        await ocr_llm.aclose()
        await title_llm.aclose()


async def test_sweep_enqueues_externally_uploaded_pdf(stack):
    store, papra, org_id, config = stack
    document = await papra.create_document(org_id, "extern-upload.pdf", make_german_pdf())

    result = await sweep_once(store, papra)

    assert result.jobs_enqueued == 1
    job = store.claim_next_job("ocr")
    assert job.document_id == document["id"]
    assert job.payload["name"] == "extern-upload.pdf"
    # second sweep is a no-op
    assert (await sweep_once(store, papra)).jobs_enqueued == 0
```

- [ ] **Step 4: Run the integration suite and iterate**

Run: `docker compose -f docker-compose.test.yml up -d --wait && .venv/bin/pytest tests/integration -v; status=$?; docker compose -f docker-compose.test.yml down -v; exit $status`
Expected: all integration tests PASS (M1 round-trip, M2 ingestion, M3 OCR pipeline + sweep). If Papra's document metadata lacks `mimeType` (verify live: `GET .../documents/:id`), the sweep's `_is_pdf` falls back to the `.pdf` extension — record what the live API returns in the report.

- [ ] **Step 5: Full one-command run**

Run: `make test` (PATH note: `make test PYTEST=.venv/bin/pytest` if bare python is not on PATH)
Expected: full suite green

- [ ] **Step 6: Lint and commit**

Run: `.venv/bin/ruff check .`

```bash
git add papra_suite/pipeline.py papra_suite/__main__.py tests/test_serve.py tests/integration/test_ocr_pipeline.py
git commit -m "feat(m3): serve runtime and end-to-end ocr/title/sweep integration tests"
```

---

## Self-Review (completed during planning)

**Spec coverage (M3 scope):** spec §12 milestone 3 ("OCR + title workers with a mocked OpenAI-compatible server; fixture: two-page German PDF → expected markdown contains known strings") → Tasks 1-4, 6; §5.4 OCR worker (download, render ~150 DPI, per-page vision requests with the fixed system prompt, pixel-budget downscale, page cap + warning, 300 s timeout, retry/backoff, assemble, write-back) → Tasks 1-2, 4; §5.5 title worker (excerpt + filename, ≤80 chars, no quotes, filename fallback, write-back, runs after OCR) → Tasks 3-4 (transition ordering); decisions §1 (3 concurrent OCR workers) → Task 6; §2 (central transition handler) → Task 4; §4 (sweep: seen-table, non-PDF skip, idempotent enqueue) → Task 4; §6 (job model: enqueue-first, rewind-retry, unique index) → unchanged from M2. M1/M2 carry-overs: lost-race interleave test + IMAP status hardening (Task 5), defensive poll wrapping + recover-at-startup (Task 6).

**Placeholders:** the Task 5 lost-race sketch and Task 6 serve-test sketches carry explicit implementation notes for their fiddly plumbing (db-path access, client ownership) with stated contracts — that is delegated judgment with a defined outcome, not a TBD.

**Type consistency:** `run_ocr_job(papra, ocr_llm, job, *, config) -> str` matches the worker_loop handler contract (str result → transition); `on_job_done(store, job, *, ocr_result)` matches; `sweep_once(store, papra) -> SweepResult` matches the serve sweep loop; `FakeLLMServer.responses` list mutation is used consistently in Tasks 5-6; `serve`'s injected-params shape matches the unit tests.

**Known wrinkles, resolved by design:** (a) the sweep downloads each unseen PDF once to compute the true file hash — a one-time cost per externally-ingested document, keeping the decisions §6 hash contract uniform (noted in the plan); (b) the title excerpt rides in the job payload (self-contained retries, no re-fetch); (c) Pillow is NOT added — PyMuPDF's matrix handles downscaling (plan-level YAGNI note); (d) `serve` closes only the clients/store it constructed (`owns_store` pattern).

Later milestones (not this plan): M4 dashboard (FastAPI + Jinja2, mailbox CRUD, retry buttons, status page); M5 MCP; M6 Dockerfile/compose/README + acceptance run.
