"""Corrected SDK probe: assert a SUCCESSFUL session before drawing conclusions.

Fixes two flaws in spike #2 that the advisor caught:
  - the "session id without credentials" request never carried the session id
  - the query-string-token request also sent a valid Authorization header
"""
import asyncio, json
import hmac
import httpx2, pydantic
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.server import MCPServer
from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.server.auth.settings import AuthSettings
from mcp.server.transport_security import TransportSecuritySettings as TSS

TS = TSS(enable_dns_rebinding_protection=True, allowed_hosts=["127.0.0.1", "127.0.0.1:80"], allowed_origins=[])
H = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}
INIT = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
    "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "p", "version": "0"}}}


class Hit(pydantic.BaseModel):
    id: str
    name: str


class Out(pydantic.BaseModel):
    results: list[Hit]
    partial: bool = False


TOKEN = "t" * 40


class V(TokenVerifier):
    async def verify_token(self, token: str) -> AccessToken | None:
        if not hmac.compare_digest(token, TOKEN):
            return None
        return AccessToken(token=token, client_id="device-a", scopes=["papra:read"])


srv = MCPServer(name="shapes", token_verifier=V(),
                auth=AuthSettings(issuer_url="http://127.0.0.1:80", resource_server_url=None,
                                  validate_token_resource=False))

@srv.tool()
async def bare_dict(q: str) -> dict:
    """dict return."""
    return {"results": [{"id": "1", "name": "n"}], "tokens_used": ["q"], "extra": "kept"}

@srv.tool()
async def model_out(q: str) -> Out:
    """pydantic return."""
    return Out(results=[Hit(id="1", name="n")])

@srv.tool()
async def opt_args(q: str, limit: int = 10, org_id: str | None = None) -> Out:
    """optional args."""
    return Out(results=[])


async def shapes():
    app = srv.streamable_http_app(json_response=True, transport_security=TS)
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app),
                                      base_url="http://127.0.0.1:80",
                                      headers={"Authorization": f"Bearer {TOKEN}"}) as hc:
            async with streamable_http_client("http://127.0.0.1:80/mcp", http_client=hc) as (r, w):
                async with ClientSession(r, w) as s:
                    init = await s.initialize()
                    print("INIT OK:", init.server_info.name)
                    tools = (await s.list_tools()).tools
                    for t in tools:
                        props = t.input_schema.get("properties", {})
                        print(f"  {t.name:10} required={t.input_schema.get('required')} "
                              f"types={ {k: v.get('type') for k, v in props.items()} } "
                              f"outputSchema={'Y' if t.output_schema else 'N'}")
                    for name, args in (("bare_dict", {"q": "x"}), ("model_out", {"q": "x"}),
                                       ("opt_args", {"q": "x"})):
                        res = await s.call_tool(name, args)
                        print(f"  {name}: is_error={res.is_error} "
                              f"structured={'None' if res.structured_content is None else json.dumps(res.structured_content)[:70]} "
                              f"text={res.content[0].text[:70]!r}")
                    # error path
                    try:
                        await s.call_tool("bare_dict", {})   # missing required arg
                        print("  missing-arg: NO ERROR RAISED")
                    except Exception as e:
                        print("  missing-arg ->", type(e).__name__, str(e)[:80])


async def auth_hygiene():
    app = srv.streamable_http_app(json_response=True, transport_security=TS)
    async with app.router.lifespan_context(app):
        async with httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app),
                                      base_url="http://127.0.0.1:80") as c:
            r0 = await c.post("/mcp", json=INIT, headers=H)
            print("no-auth init ->", r0.status_code)
            r = await c.post("/mcp", json=INIT, headers={**H, "Authorization": f"Bearer {TOKEN}"})
            print("right-token init ->", r.status_code)
            # server has no token verifier, so a valid-looking request is accepted:
            sid = dict(r.headers).get("mcp-session-id")
            print("session id from init:", repr(sid)[:24])
            notif = {"jsonrpc": "2.0", "method": "notifications/initialized"}
            r2 = await c.post("/mcp", json=notif, headers={**H, "mcp-session-id": sid or "", "Authorization": f"Bearer {TOKEN}"})
            print("notification with session id ->", r2.status_code)
            body2 = {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
            r3 = await c.post("/mcp", json=body2, headers={**H, "mcp-session-id": sid or ""})
            print("tools/list WITH session id, NO auth ->", r3.status_code)
            r4 = await c.post("/mcp", json=body2, headers={**H, "mcp-session-id": sid or "", "Authorization": f"Bearer {TOKEN}"})
            print("tools/list WITH session id, WITH auth ->", r4.status_code)
            r5 = await c.post("/mcp?access_token=" + TOKEN, json=INIT, headers=H)
            print("init with token ONLY in query string, no header ->", r5.status_code)
            r6 = await c.delete("/mcp", headers={**H, "mcp-session-id": sid or ""})
            print("DELETE with session id, NO auth ->", r6.status_code)
            print("tools/list with session id ->", r3.status_code)


async def main():
    await shapes()
    print()
    await auth_hygiene()

asyncio.run(main())
