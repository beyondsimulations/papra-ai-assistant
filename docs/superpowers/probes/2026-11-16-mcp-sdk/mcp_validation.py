"""What does the SDK actually enforce about tool arguments?"""
import asyncio, httpx2
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.server import MCPServer
from mcp.server.transport_security import TransportSecuritySettings as TSS

TS = TSS(enable_dns_rebinding_protection=True, allowed_hosts=["127.0.0.1"], allowed_origins=[])
srv = MCPServer(name="v")

@srv.tool()
async def search(query: str, limit: int = 10, org_id: str | None = None) -> dict:
    """Search."""
    return {"got_query_type": type(query).__name__, "limit": limit,
            "org_id_type": type(org_id).__name__}

async def main():
    app = srv.streamable_http_app(json_response=True, transport_security=TS)
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app),
                                      base_url="http://127.0.0.1") as hc:
            async with streamable_http_client("http://127.0.0.1/mcp", http_client=hc) as (r, w):
                async with ClientSession(r, w) as s:
                    await s.initialize()
                    t = (await s.list_tools()).tools[0]
                    print("schema:", t.input_schema)
                    cases = [
                        ("missing required", {}),
                        ("right types", {"query": "miete", "limit": 3}),
                        ("wrong types", {"query": 42, "limit": "many", "org_id": 7}),
                        ("limit 0", {"query": "a", "limit": 0}),
                        ("limit huge", {"query": "a", "limit": 10**9}),
                        ("extra unknown key", {"query": "a", "password": "x"}),
                        ("query 10k chars", {"query": "x" * 10_000}),
                    ]
                    for label, args in cases:
                        try:
                            res = await s.call_tool("search", args)
                            print(f"  {label:20} is_error={res.is_error} -> {res.content[0].text[:90]}")
                        except Exception as e:
                            print(f"  {label:20} RAISED {type(e).__name__}: {str(e)[:70]}")
asyncio.run(main())
