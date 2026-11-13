import base64
import logging

import httpx
import pytest

from papra_suite.llm_client import LLMClient, LLMError

# sentinel: the finish_reason KEY is absent from the response body
OMITTED = object()


def completion_body(content: str, finish_reason=OMITTED) -> dict:
    choice: dict = {"message": {"content": content}}
    if finish_reason is not OMITTED:
        choice["finish_reason"] = finish_reason
    return {"choices": [choice]}


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
    assert seen["path"] == "/v1/chat/completions"
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


async def test_image_attaches_to_last_user_message():
    """With TWO user messages, the image part must land on the LAST one —
    dropping reversed() (first-user-message mutant) fails this test."""
    import json

    bodies = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.read())
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    async with make_client(handler) as client:
        await client.chat(
            [
                {"role": "system", "content": "sys"},
                {"role": "user", "content": "first question"},
                {"role": "assistant", "content": "an answer"},
                {"role": "user", "content": "and this one"},
            ],
            image=b"\x89PNG fake",
        )

    sent = json.loads(bodies[0])["messages"]
    expected = "data:image/png;base64," + base64.b64encode(b"\x89PNG fake").decode()
    # the SECOND user message carries the image part...
    assert sent[3]["content"] == [
        {"type": "text", "text": "and this one"},
        {"type": "image_url", "image_url": {"url": expected}},
    ]
    # ...and the first user message (and system) are untouched
    assert sent[1] == {"role": "user", "content": "first question"}
    assert sent[0] == {"role": "system", "content": "sys"}


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


# --- Pins required by the Task 1 review: previously unpinned branches ---


async def test_429_is_retried():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429)
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    async with make_client(handler) as client:
        assert await client.chat([{"role": "user", "content": "x"}]) == "ok"
    assert calls["n"] == 2


async def test_http_exhaustion_raises_with_status():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503)

    async with make_client(handler) as client:
        with pytest.raises(LLMError) as excinfo:
            await client.chat([{"role": "user", "content": "x"}])
    assert excinfo.value.status == 503
    assert calls["n"] == 3


async def test_timeout_is_passed_to_underlying_client():
    # httpx 0.28: a scalar timeout populates all four Timeout attributes,
    # so timeout.connect pins the pass-through
    client = LLMClient("http://llm.test", "k", "m", timeout_s=12.5)
    try:
        assert client._client.timeout.connect == 12.5
    finally:
        await client.aclose()


async def test_image_without_user_message_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

    async with make_client(handler) as client:
        with pytest.raises(LLMError):
            await client.chat([{"role": "system", "content": "sys"}], image=b"x")


async def test_malformed_invalid_json_body_raises_llm_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>not json</html>")

    async with make_client(handler) as client:
        with pytest.raises(LLMError, match="malformed chat completions response"):
            await client.chat([{"role": "user", "content": "hi"}])


async def test_malformed_missing_choices_key_raises_llm_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": "shape"})

    async with make_client(handler) as client:
        with pytest.raises(LLMError, match="malformed chat completions response"):
            await client.chat([{"role": "user", "content": "hi"}])


async def test_malformed_empty_choices_list_raises_llm_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": []})

    async with make_client(handler) as client:
        with pytest.raises(LLMError, match="malformed chat completions response"):
            await client.chat([{"role": "user", "content": "hi"}])


async def test_malformed_non_utf8_body_raises_llm_error():
    # invalid UTF-8 (not a BOM) escapes json.loads as UnicodeDecodeError —
    # the parse wrap must convert it, not leak it
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"\x80\x81\x82 not utf8 \xff")

    async with make_client(handler) as client:
        with pytest.raises(LLMError, match="malformed chat completions response") as excinfo:
            await client.chat([{"role": "user", "content": "hi"}])
    assert excinfo.value.status == 200


async def test_malformed_message_as_string_raises_llm_error():
    # message is a string, not a dict: ["content"] raises TypeError
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": [{"message": "plain string"}]})

    async with make_client(handler) as client:
        with pytest.raises(LLMError, match="malformed chat completions response") as excinfo:
            await client.chat([{"role": "user", "content": "hi"}])
    assert excinfo.value.status == 200


# --- Truncated completions: a 200 that stopped at the output cap is NOT a success ---


async def test_finish_reason_length_is_permanent_error_without_retry():
    """A released mlx_lm.server answers HTTP 200 with finish_reason="length"
    when it hits its output cap. That half page must never be handed back as a
    completion: permanent LLMError, status None, and EXACTLY ONE request (the
    client's own retry loop must not re-fire on a deterministic truncation)."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=completion_body("half a page", "length"))

    async with make_client(handler) as client:
        with pytest.raises(LLMError) as excinfo:
            await client.chat([{"role": "user", "content": "x"}])
    assert excinfo.value.status is None
    assert excinfo.value.permanent is True
    assert calls["n"] == 1
    # the message names the finish_reason and the content length it cut off at
    assert "length" in str(excinfo.value)
    assert str(len("half a page")) in str(excinfo.value)


async def test_finish_reason_content_filter_is_permanent_like_length():
    """content_filter ends generation early too — same permanent failure."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=completion_body("blocked", "content_filter"))

    async with make_client(handler) as client:
        with pytest.raises(LLMError) as excinfo:
            await client.chat([{"role": "user", "content": "x"}])
    assert excinfo.value.status is None and excinfo.value.permanent is True
    assert calls["n"] == 1
    assert "content_filter" in str(excinfo.value)


@pytest.mark.parametrize("finish_reason", [OMITTED, None, "stop"])
async def test_untruncated_completion_returns_content(finish_reason):
    """Absent (our fake, and any server that omits the field), explicit null,
    and "stop" are all success — treating absent as truncation would break
    every server that does not send the field."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=completion_body("  full page\n", finish_reason))

    async with make_client(handler) as client:
        assert await client.chat([{"role": "user", "content": "x"}]) == "full page"


async def test_unknown_finish_reason_warns_but_returns_content(caplog):
    """An unrecognised reason is not treated as truncation (content stays
    usable) but must be visible in the logs instead of silently accepted."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=completion_body("page", "tool_calls"))

    with caplog.at_level(logging.WARNING):
        async with make_client(handler) as client:
            assert await client.chat([{"role": "user", "content": "x"}]) == "page"
    assert any("tool_calls" in record.getMessage() for record in caplog.records)
    assert all(record.levelno < logging.ERROR for record in caplog.records)


async def test_malformed_top_level_array_raises_llm_error():
    # a top-level JSON array: ["choices"] on a list raises TypeError
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[1, 2, 3])

    async with make_client(handler) as client:
        with pytest.raises(LLMError, match="malformed chat completions response") as excinfo:
            await client.chat([{"role": "user", "content": "hi"}])
    assert excinfo.value.status == 200
