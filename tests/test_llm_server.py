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


async def test_fake_server_rejects_off_route_without_consuming_script():
    """Anti-recurrence strictness: the fake serves ONLY the standard
    /v1/chat/completions route (vLLM, Ollama and LM Studio document nothing
    else), and a request to any other path is a 404 that consumes NO scripted
    response and is NOT counted as a completion request. A route-blind fake is
    what let a client posting the root /chat/completions look healthy."""
    server = FakeLLMServer()
    server.responses.extend(["erste Seite", "zweite Seite"])
    server.start()
    try:
        async with httpx.AsyncClient() as http:
            off_route = await http.post(
                f"{server.url}/chat/completions", json={"messages": []})
        assert off_route.status_code == 404
        # rejected off-route request: no completion recorded, script untouched
        assert server.requests == []
        assert server.off_route_paths == ["/chat/completions"]
        # ...and the script is still at its START: two DISTINCT scripted answers
        # mean a rejected request that advanced the script would hand back
        # "zweite Seite" first and fail here (one repeated answer could not tell)
        async with LLMClient(server.url, "k", "m", backoff_base_s=0.0) as llm:
            assert await llm.chat([{"role": "user", "content": "a"}]) == "erste Seite"
            assert await llm.chat([{"role": "user", "content": "b"}]) == "zweite Seite"
        assert len(server.requests) == 2
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
