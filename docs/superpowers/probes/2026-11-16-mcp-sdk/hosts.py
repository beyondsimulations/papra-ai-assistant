import asyncio, httpx2
from mcp.server import MCPServer
srv = MCPServer(name="h")
body = {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}
H = {"Accept":"application/json, text/event-stream","Content-Type":"application/json"}
async def main():
    app = srv.streamable_http_app(json_response=True)   # NO transport_security -> defaults
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url="http://x") as c:
            for host in ["127.0.0.1","127.0.0.1:80","localhost","localhost:80","[::1]","[::1]:80",
                         "0.0.0.0","host.ts.net","127.0.0.1.localhost"]:
                r = await c.post("/mcp", json=body, headers={**H, "Host": host})
                print(f"Host {host:22} -> {r.status_code}")
asyncio.run(main())
