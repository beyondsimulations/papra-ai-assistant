import httpx
import pytest

from papra_suite.papra_client import PapraClient, PapraError

ORG = "org_123"
DOC = {"id": "doc_1", "name": "bill.pdf", "mimeType": "application/pdf"}


def make_client(handler, **kwargs):
    return PapraClient(
        "http://papra.test", "ppapi_test", backoff_base_s=0.0,
        transport=httpx.MockTransport(handler), **kwargs,
    )


async def test_bearer_auth_header_is_sent():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("Authorization")
        return httpx.Response(200, json={"document": DOC})

    async with make_client(handler) as client:
        await client.get_document(ORG, "doc_1")
    assert seen["auth"] == "Bearer ppapi_test"


async def test_retries_5xx_then_succeeds():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(500)
        return httpx.Response(200, json={"document": DOC})

    async with make_client(handler) as client:
        document = await client.get_document(ORG, "doc_1")
    assert document["id"] == "doc_1" and calls["n"] == 3


async def test_4xx_raises_immediately_without_retry():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(404, json={"code": "document.not_found"})

    async with make_client(handler) as client:
        with pytest.raises(PapraError) as excinfo:
            await client.get_document(ORG, "doc_1")
    assert excinfo.value.status == 404
    assert calls["n"] == 1


async def test_429_is_retried():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429)
        return httpx.Response(200, json={"document": DOC})

    async with make_client(handler) as client:
        await client.get_document(ORG, "doc_1")
    assert calls["n"] == 2


async def test_network_error_retried_then_raises():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        raise httpx.ConnectError("connection refused")

    async with make_client(handler) as client:
        with pytest.raises(PapraError, match="connection refused"):
            await client.get_document(ORG, "doc_1")
    assert calls["n"] == 3


async def test_update_document_sends_only_provided_fields():
    bodies = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.read())
        return httpx.Response(200, json={"document": DOC})

    async with make_client(handler) as client:
        await client.update_document(ORG, "doc_1", name="New title")
        await client.update_document(ORG, "doc_1", content="# markdown")
    import json

    assert json.loads(bodies[0]) == {"name": "New title"}
    assert json.loads(bodies[1]) == {"content": "# markdown"}


async def test_list_documents_passes_search_query():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["params"] = dict(request.url.params)
        return httpx.Response(200, json={"documents": [DOC], "documentsCount": 1})

    async with make_client(handler) as client:
        documents, count = await client.list_documents(ORG, search_query="invoice")
    assert documents == [DOC] and count == 1
    assert seen["params"]["searchQuery"] == "invoice"


async def test_download_file_returns_bytes():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"%PDF-1.4 fake")

    async with make_client(handler) as client:
        content = await client.download_file(ORG, "doc_1")
    assert content == b"%PDF-1.4 fake"


async def test_add_tag_posts_tag_id():
    bodies = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.read())
        return httpx.Response(204)

    async with make_client(handler) as client:
        await client.add_tag(ORG, "doc_1", "tag_9")
    import json

    assert json.loads(bodies[0]) == {"tagId": "tag_9"}
