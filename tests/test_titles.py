import httpx
import pytest

from papra_suite.llm_client import LLMClient
from papra_suite.titles import (
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
