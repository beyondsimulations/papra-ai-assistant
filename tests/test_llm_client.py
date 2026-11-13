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
