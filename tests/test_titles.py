import httpx

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


async def test_generate_title_falls_back_on_llm_failure(caplog):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    llm = LLMClient("http://llm.test", "k", "m", backoff_base_s=0.0,
                    transport=httpx.MockTransport(handler))
    title = await generate_title(llm, filename="rechnung-42.pdf", excerpt="inhalt")
    assert title == "rechnung-42.pdf"
    assert any("failed" in r.getMessage().lower() for r in caplog.records)
