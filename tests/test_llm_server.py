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
