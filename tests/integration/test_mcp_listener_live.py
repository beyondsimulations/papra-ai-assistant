"""Task 12: the MCP listener on a real socket, driven by the SDK's own client.

Everything in `tests/test_mcp_server_*.py` drives the ASGI app through starlette's `TestClient`,
which is an in-process transport: it never binds a port, never parses a `Host` header off the wire,
and never runs uvicorn's lifespan. That leaves the three things this file exists to prove unproven:

* **the socket binds where it says it binds** — loopback, and the port `serve` was configured with;
* **the SDK's real client can complete a session** — `initialize`, `tools/list`, `tools/call` — over
  Streamable HTTP, including the `mcp-session-id` and `Accept` negotiation a fake transport lets us
  get wrong silently;
* **`run_mcp_listener` stops when asked** — `serve` shares the process with the pollers, so a
  listener that will not shut down is a hung container, not an inconvenience.

Marked `integration` because it binds a socket, but it needs no compose stack: the peer is Kreuzakt
substituted in-process, and the one caller that matters (`index_status`) is answered from a temp
store. It is deliberately not the atlas/Tailscale deployment check — publishing over `tailscale
serve` is a runbook item, and a test that passed here and failed there would be worse than none.
"""

import socket
import time

import httpx2
import pytest
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.exceptions import MCPError

from papra_suite.mcp_server import run_mcp_listener
from papra_suite.mcp_tokens import mint_token
from papra_suite.store import Store
from tests.test_config import BASE_ENV

#: JSON-RPC'S INTERNAL_ERROR: how the SDK reports “the server answered an error response”,
#: which is what a mid-session 401 looks like from the client side.
JSONRPC_INTERNAL_ERROR = -32603

pytestmark = pytest.mark.integration

PUBLIC_URL = "https://papra-mcp.test"
HOST = "papra-mcp.test"
ORG = "org-live"


class LiveKreuzakt:
    """The two calls `index_status` makes. Counted, so a leak would show as a call."""

    def __init__(self):
        self.queue_calls = 0

    async def health(self):
        return {"status": "ok", "documents": 1}

    async def queue_counts(self):
        self.queue_calls += 1
        return {"pending": 7, "completed": 3}


def free_port():
    """An ephemeral port, released before the listener takes it.

    Port 0 would be cleaner, but uvicorn does not report the port it was given back to us, and a
    test that asserts against a port it cannot read proves nothing.
    """
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture
async def listener(tmp_path):
    """The real `serve` path — `run_mcp_listener` — on 127.0.0.1, with a store of its own."""
    import asyncio
    from types import SimpleNamespace

    store = Store(tmp_path / "state.db")
    store.migrate()
    store.ledger_upsert_reference(papra_org_id=ORG, papra_document_id="d1", file_hash="cd" * 32,
                                  papra_original_name="d1.pdf")
    port = free_port()
    config = SimpleNamespace(**dict(BASE_ENV, DATA_DIR=str(tmp_path)),
                             mcp_host="127.0.0.1", mcp_port=port, mcp_url=PUBLIC_URL)
    kreuzakt = LiveKreuzakt()
    stop = asyncio.Event()
    task = asyncio.create_task(run_mcp_listener(config, store, papra=object(), kreuzakt=kreuzakt,
                                                stop=stop))
    endpoint = f"http://127.0.0.1:{port}/mcp"
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), 0.25):
                break
        except OSError:
            await asyncio.sleep(0.05)
    else:
        pytest.fail("the mcp listener never took the port")

    try:
        yield SimpleNamespace(endpoint=endpoint, port=port, store=store, kreuzakt=kreuzakt)
    finally:
        stop.set()
        # Bounded, and a failure here is the test's own: a listener that outlives its stop event is
        # the hung-container bug, so it fails the run rather than leaking a task.
        await asyncio.wait_for(task, timeout=20.0)
    store.close()


def raw_client(token=None):
    headers = {"host": HOST}
    if token:
        headers["authorization"] = f"Bearer {token}"
    return httpx2.AsyncClient(headers=headers, timeout=10.0)


async def test_the_socket_answers_on_loopback_and_refuses_the_wrong_host(listener):
    """The proxy-facing half of the deployment, over TCP rather than in-process.

    `tailscale serve` rewrites the request and presents its own `Host`; that is the case the 421
    exists for, and it is only observable with a real socket and a header we choose by hand.
    """
    async with raw_client() as client:
        proxied = await client.post(listener.endpoint, json={},
                                    headers={"host": "some-other-name.example"})
        plain = await client.post(listener.endpoint, json={})
    assert proxied.status_code == 421, "a spoofed Host must be refused before anything else"
    assert plain.status_code == 401        # the listener itself is reachable and named


async def test_missing_wrong_and_query_string_credentials_are_the_same_401(listener):
    token = mint_token(listener.store, label="live", org_ids=[ORG])
    async with raw_client() as client:
        missing = await client.post(listener.endpoint, json={})
        wrong = await client.post(listener.endpoint, json={},
                                  headers={"authorization": "Bearer not-a-token"})
        query = await client.get(f"{listener.endpoint}?access_token={token}")
    assert missing.status_code == wrong.status_code == query.status_code == 401
    assert missing.content == wrong.content == query.content, "the 401s must be byte-identical"
    assert token not in query.text and token not in missing.headers.get("www-authenticate", "")


async def test_an_unlisted_origin_is_refused_over_the_wire(listener):
    token = mint_token(listener.store, label="origin", org_ids=[ORG])
    async with raw_client(token) as client:
        response = await client.post(listener.endpoint, json={},
                                     headers={"origin": "https://evil.example"})
    assert response.status_code == 403


async def test_the_sdk_client_completes_a_session_and_calls_the_tool(listener):
    """The client that matters is the SDK's own, not a hand-rolled JSON-RPC exchange.

    Negotiating the protocol version, echoing `mcp-session-id`, and reading structured tool output
    are all SDK behaviour; a hand-rolled client would test our parsing of our own request shapes and
    miss a client that cannot get in the door.
    """
    token = mint_token(listener.store, label="sdk", org_ids=[ORG])
    async with raw_client(token) as http:
        async with Client(streamable_http_client(listener.endpoint, http_client=http)) as client:
            tools = await client.list_tools()
            assert [tool.name for tool in tools.tools] == ["index_status"]
            result = await client.call_tool("index_status")

    payload = result.structured_content
    assert payload["scope"] == "buckets"
    assert [bucket["org_id"] for bucket in payload["buckets"]] == [ORG]
    # The brief's leak, asserted over a socket: a one-bucket credential does not learn the corpus's
    # queue depth, and the server never asked Kreuzakt for it.
    assert payload["queue"] is None
    assert listener.kreuzakt.queue_calls == 0


async def test_an_all_buckets_token_gets_the_queue_over_the_wire_too(listener):
    token = mint_token(listener.store, label="sdk-operator", org_ids=[], scope_all=True)
    async with raw_client(token) as http:
        async with Client(streamable_http_client(listener.endpoint, http_client=http)) as client:
            payload = (await client.call_tool("index_status")).structured_content
    assert payload["scope"] == "all"
    assert payload["queue"] == {"pending": 7, "completed": 3}


async def test_revoking_a_token_stops_a_session_that_is_already_open(listener):
    """The scope is read per request, so `--revoke` is immediate — no restart, no cache to flush."""
    token = mint_token(listener.store, label="laptop", org_ids=[ORG])
    from papra_suite.mcp_tokens import revoke_token

    async with raw_client(token) as http:
        async with Client(streamable_http_client(listener.endpoint, http_client=http)) as client:
            assert (await client.call_tool("index_status")).structured_content["scope"] == "buckets"
            revoke_token(listener.store, label="laptop")
            with pytest.raises(MCPError) as refused:
                await client.call_tool("index_status")
    # What the SDK reports is its own translation, not the HTTP status; the status is asserted
    # directly, on a fresh request carrying the same revoked credential and no session at all.
    assert refused.value.code == JSONRPC_INTERNAL_ERROR
    async with raw_client(token) as client:
        after = await client.post(listener.endpoint, json={})
    assert after.status_code == 401


def test_nothing_listens_until_the_setting_enables_it(tmp_path):
    """The port is a decision, not a default: a fresh store asks for no listener."""
    from papra_suite.mcp_server import mcp_listener_requested

    store = Store(tmp_path / "state.db")
    store.migrate()
    try:
        assert mcp_listener_requested(store) is False
    finally:
        store.close()
    # And by absence: a port nobody bound refuses a connection.
    port = free_port()
    with pytest.raises(OSError):
        with socket.create_connection(("127.0.0.1", port), 0.25):
            pass
