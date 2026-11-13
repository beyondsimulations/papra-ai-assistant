import time

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
        seen["path"] = request.url.path
        seen["method"] = request.method
        return httpx.Response(200, json={"document": DOC})

    async with make_client(handler) as client:
        await client.get_document(ORG, "doc_1")
    assert seen["auth"] == "Bearer ppapi_test"
    assert seen["method"] == "GET"
    assert seen["path"] == "/api/organizations/org_123/documents/doc_1"


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


async def test_backoff_sleeps_between_attempts():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(500)
        return httpx.Response(200, json={"document": DOC})

    client = PapraClient(
        "http://papra.test", "ppapi_test", backoff_base_s=0.05,
        transport=httpx.MockTransport(handler),
    )
    start = time.monotonic()
    async with client:
        await client.get_document(ORG, "doc_1")
    elapsed = time.monotonic() - start
    assert calls["n"] == 3
    assert elapsed >= 0.24  # two sleeps: 0.05 + 0.20 = 0.25s total


async def test_501_is_retried_then_succeeds():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(501)
        return httpx.Response(200, json={"document": DOC})

    async with make_client(handler) as client:
        document = await client.get_document(ORG, "doc_1")
    assert document["id"] == "doc_1" and calls["n"] == 2


async def test_persistent_5xx_raises_with_status():
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503)

    async with make_client(handler) as client:
        with pytest.raises(PapraError) as excinfo:
            await client.get_document(ORG, "doc_1")
    assert excinfo.value.status == 503
    assert calls["n"] == 3


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
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.read())
        seen["method"] = request.method
        seen["path"] = request.url.path
        return httpx.Response(200, json={"document": DOC})

    async with make_client(handler) as client:
        await client.update_document(ORG, "doc_1", name="New title")
        await client.update_document(ORG, "doc_1", content="# markdown")
    assert seen["method"] == "PATCH"
    assert seen["path"] == "/api/organizations/org_123/documents/doc_1"
    import json

    assert json.loads(bodies[0]) == {"name": "New title"}
    assert json.loads(bodies[1]) == {"content": "# markdown"}


async def test_list_documents_passes_search_query():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["params"] = dict(request.url.params)
        seen["path"] = request.url.path
        return httpx.Response(200, json={"documents": [DOC], "documentsCount": 7})

    async with make_client(handler) as client:
        documents, count = await client.list_documents(ORG, search_query="invoice")
    # count is the server-reported total (7), not len(documents) (1) —
    # pinning the documentsCount contract against a len() fallback mutation.
    assert documents == [DOC] and count == 7
    assert seen["path"] == "/api/organizations/org_123/documents"
    # Pin the full pagination contract: dropping the params dict entirely
    # (or any of the five keys) must fail the suite.
    assert seen["params"] == {
        "pageIndex": "0",
        "pageSize": "100",
        "sortField": "createdAt",
        "sortOrder": "desc",
        "searchQuery": "invoice",
    }


async def test_list_documents_falls_back_when_count_missing():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"documents": [DOC]})

    async with make_client(handler) as client:
        documents, count = await client.list_documents(ORG)
    # Without documentsCount the documented fallback is len(documents).
    assert documents == [DOC] and count == 1


async def test_download_file_returns_bytes():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        return httpx.Response(200, content=b"%PDF-1.4 fake")

    async with make_client(handler) as client:
        content = await client.download_file(ORG, "doc_1")
    assert content == b"%PDF-1.4 fake"
    assert seen["path"] == "/api/organizations/org_123/documents/doc_1/file"


async def test_add_tag_posts_tag_id():
    bodies = []
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.read())
        seen["method"] = request.method
        seen["path"] = request.url.path
        return httpx.Response(204)

    async with make_client(handler) as client:
        await client.add_tag(ORG, "doc_1", "tag_9")
    import json

    assert json.loads(bodies[0]) == {"tagId": "tag_9"}
    assert seen["method"] == "POST"
    assert seen["path"] == "/api/organizations/org_123/documents/doc_1/tags"


async def test_create_document_sends_multipart_file():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["content_type"] = request.headers.get("Content-Type", "")
        seen["body"] = request.read()
        seen["method"] = request.method
        seen["path"] = request.url.path
        return httpx.Response(200, json={"document": DOC})

    async with make_client(handler) as client:
        document = await client.create_document(ORG, "bill.pdf", b"%PDF-1.4 fake")

    assert document == DOC
    assert seen["method"] == "POST"
    assert seen["path"] == "/api/organizations/org_123/documents"
    assert seen["content_type"].startswith("multipart/form-data")
    body = seen["body"]
    assert b'name="file"' in body  # part must be named "file"
    assert b'filename="bill.pdf"' in body
    assert b"%PDF-1.4 fake" in body


async def test_list_tags():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["method"] = request.method
        return httpx.Response(200, json={"tags": [{"id": "tag_1", "name": "invoice"}]})

    async with make_client(handler) as client:
        tags = await client.list_tags(ORG)

    assert seen["path"] == "/api/organizations/org_123/tags"
    assert seen["method"] == "GET"
    assert tags == [{"id": "tag_1", "name": "invoice"}]


async def test_whoami_returns_api_key():
    api_key = {"id": "ak_1", "permissions": []}
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        return httpx.Response(200, json={"apiKey": api_key})

    async with make_client(handler) as client:
        assert await client.whoami() == api_key
    # Pin the whoami endpoint path against a mutated URL.
    assert seen["path"] == "/api/api-keys/current"
