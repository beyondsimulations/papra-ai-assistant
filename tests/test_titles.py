import httpx
import pytest

from papra_suite.llm_client import LLMClient
from papra_suite.titles import (
    EXCERPT_CHARS,
    TITLE_MAX_LENGTH,
    build_title_messages,
    generate_title,
    sanitize_title,
)


def test_build_title_messages_shape():
    messages = build_title_messages(filename="rechnung-42.pdf", excerpt="Rechnung Nr. 42")
    assert messages[0]["role"] == "system"
    assert "maximum 80 characters" in messages[0]["content"]
    assert messages[1]["role"] == "user"
    assert "rechnung-42.pdf" in messages[1]["content"]
    assert "Rechnung Nr. 42" in messages[1]["content"]


def test_build_title_messages_truncates_excerpt():
    """Excerpt-BUDGET pin (was a length-only pin, satisfied by any stub slice):
    kills `trimmed = excerpt[:EXCERPT_CHARS]` -> `excerpt[:20]` / `[:0]`, which
    would generate every title of a long document from a sliver of page 1 while
    the suite stayed green. The tail marker sits just inside the budget, so a
    shorter slice cannot produce it; the filler proves the budget still cuts."""
    long = "A" * (EXCERPT_CHARS - 60) + "TAILMARKER" + "B" * 3000
    messages = build_title_messages(filename="f.pdf", excerpt=long)
    content = messages[1]["content"]
    assert len(content) < 2600  # the excerpt is truncated (the old assertion)
    assert "TAILMARKER" in content  # ... at EXCERPT_CHARS, not after 20 chars
    assert "B" * 60 not in content  # the tail beyond the budget is gone


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
        content = '  "Rechnung 42 – Möbel"\n'
        return httpx.Response(200, json={"choices": [{"message": {"content": content}}]})

    llm = LLMClient("http://llm.test", "k", "m", transport=httpx.MockTransport(handler))
    title = await generate_title(llm, filename="x.pdf", excerpt="inhalt")
    assert title == "Rechnung 42 – Möbel"


async def test_generate_title_empty_llm_response_uses_filename_fallback():
    """Empty-response pin: a 2xx with EMPTY content is not an error, but the
    sanitize chain must still yield the (sanitized) filename fallback —
    breaking the fallback chain (e.g. sanitize_title(raw) with no fallback,
    which yields "Untitled") fails this test."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": [{"message": {"content": ""}}]})

    llm = LLMClient("http://llm.test", "k", "m", transport=httpx.MockTransport(handler))
    # quotes/whitespace make the chain observable: the fallback must be the
    # SANITIZED filename, not the raw one
    title = await generate_title(llm, filename='  "Rechnung 42.pdf"  ', excerpt="inhalt")
    assert title == "Rechnung 42.pdf"


async def test_generate_title_falls_back_on_llm_failure(caplog):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    llm = LLMClient("http://llm.test", "k", "m", backoff_base_s=0.0,
                    transport=httpx.MockTransport(handler))
    title = await generate_title(llm, filename="rechnung-42.pdf", excerpt="inhalt")
    assert title == "rechnung-42.pdf"
    assert any("failed" in r.getMessage().lower() for r in caplog.records)


@pytest.mark.parametrize("response", [
    # non-UTF-8 2xx body (UnicodeDecodeError at parse time)
    httpx.Response(200, content=b"\x80\x81\x82 not utf8 \xff"),
    # message as a string instead of a dict (TypeError)
    httpx.Response(200, json={"choices": [{"message": "plain string"}]}),
    # top-level JSON array (TypeError)
    httpx.Response(200, json=[1, 2, 3]),
], ids=["non-utf8", "message-as-string", "top-level-array"])
async def test_generate_title_falls_back_on_malformed_2xx(response):
    """Any malformed 2xx body must surface as LLMError so the filename
    fallback engages — a raw UnicodeDecodeError/TypeError would instead
    kill the title job permanently."""
    responses = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        responses["n"] += 1
        return response

    llm = LLMClient("http://llm.test", "k", "m", backoff_base_s=0.0,
                    transport=httpx.MockTransport(handler))
    title = await generate_title(llm, filename="rechnung-42.pdf", excerpt="inhalt")
    assert title == "rechnung-42.pdf"
    assert responses["n"] == 1  # a 2xx parse failure is permanent, not retried


async def test_generate_title_falls_back_on_a_permanent_completion():
    """C2's designed outcome, and the one class of LLMError the fallback used
    not to survive for: a completion the server cut off (finish_reason
    "length", content null or half a title) arrives as
    LLMError(permanent=True), which SKIPS LLMClient's own retry loop — so
    generate_title's `except LLMError` is the only thing between it and the
    worker's retry ladder.

    Narrowing that clause to the retryable half (the mutant this round found
    SURVIVING: `if error.permanent: raise`, on the reasonable-looking grounds
    that a permanent error should fail the job) sends the title job to
    mark_title_failed with a document that gets no title at all, instead of the
    filename it arrived with — and the suite stayed green, because every other
    fallback test drives a 500 or a malformed body, which are permanent=False.
    The fallback is the product decision: a titleless document is still a
    document.
    """
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json={"choices": [
            {"message": {"content": "Rechnung über die Miete 2026"},
             "finish_reason": "length"}]})

    llm = LLMClient("http://llm.test", "k", "m", backoff_base_s=0.0,
                    transport=httpx.MockTransport(handler))
    title = await generate_title(llm, filename="rechnung-42.pdf", excerpt="inhalt")

    assert title == "rechnung-42.pdf"   # the filename, not the truncated title
    assert calls["n"] == 1              # and asked exactly once: permanent


def test_the_title_prompt_nominates_no_language():
    """Nothing in the pipeline knows what language a forwarded document is in, so
    naming two in the prompt tells the model the third one is a mistake to correct.

    A German invoice that comes back titled "Invoice" is a document nobody finds by
    looking for the word they remember. The instruction that replaces the list —
    "in the document's own language; never translate" — is the whole of it.

    The OCR prompt carries the same assumption and is changed on its own branch, not
    this one: the plan's review gate for ingest-visibility excludes `ocr.py`, and a
    transcription prompt is a better place to argue about behaviour than inside a
    branch about ingest visibility.
    """
    from papra_suite.titles import TITLE_SYSTEM_PROMPT

    lowered = TITLE_SYSTEM_PROMPT.lower()
    assert "german" not in lowered and "english" not in lowered
    assert "own language" in lowered
    assert "never translate" in lowered
