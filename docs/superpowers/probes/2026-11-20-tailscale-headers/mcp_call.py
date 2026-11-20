"""Call one MCP tool against a running listener and print what came back.

Why this exists: after Step 6 the two questions that decide whether the listener may stay on —
"does a German scan answer to `Straße`, and to an inflected form?" and "does a scoped token see
nothing of a bucket it was not granted?" — are questions about the **wire**, not about the store.
Reading `state.db`, or asking Kreuzakt directly, answers a different question: the authorization
boundary is the ledger filter in front of the index, and it only exists inside a tool call.

So this is the smallest thing that can ask the real server. It speaks Streamable HTTP with the
standard library only — no `mcp`, no `httpx2` — because it has to run on a server that has the
production install and nothing else, and because a probe that depends on the SDK under test cannot
discover that the SDK's client is the thing that changed.

    python3 mcp_call.py --url https://papra-mcp.ts.net/mcp --token-env PAPRA_MCP_TOKEN \\
        search_documents query=Straße page_size=5

`--token-env` names an environment variable; the token is never an argument, because argv is
visible in `ps` and in a shell history file, and the token is a credential for the whole archive
that bucket grants. `--insecure-host` exists for the one legitimate local case, reading the
listener over `127.0.0.1` on the box itself, where the `Host` header has to say what the listener
allow-lists and the certificate is not the one for the public name.

A refusal is a result. `not-found`, `index_unavailable`, `unauthenticated`, `invalid-request` are
all printed as they arrive, and a non-zero exit means "the call did not return content" — which is
the answer the boundary test wants, not an error to retry.
"""

import argparse
import json
import os
import ssl
import sys
import urllib.error
import urllib.request

PROTOCOL_VERSION = "2025-06-18"
#: Both types, exactly as the SDK's client sends them. `text/event-stream` alone is refused, and
#: so is `application/json` alone, on the Streamable HTTP transport.
ACCEPT = "application/json, text/event-stream"


def _post(url: str, token: str, body: dict, session: str | None, host: str | None):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": ACCEPT,
        # urllib does not reuse connections, and an SSE response has no Content-Length: without
        # this the read waits for a keep-alive close that the server has no reason to send.
        "Connection": "close",
    }
    if session:
        headers["mcp-session-id"] = session
    if host:
        # `tailscale serve` and this listener disagree about the name unless somebody says which
        # one the Host header carries; on the box itself the operator needs to be able to choose it,
        # because that header is exactly what the 421 gate reads.
        headers["Host"] = host
    request = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers,
                                     method="POST")
    with urllib.request.urlopen(request) as response:  # noqa: S310 - https/loopback by the operator
        return response.headers.get("mcp-session-id"), _frames(response.read().decode())


def _frames(text: str) -> list[dict]:
    """The JSON payloads in an SSE body, or in a plain JSON response.

    The listener answers with `data:` lines; a proxy or a future transport may hand back one JSON
    document. Both are printed as frames, because the shape that decides the test is inside them
    either way.
    """
    if text.lstrip().startswith("{"):
        return [json.loads(text)]
    frames = []
    for line in text.splitlines():
        if line.startswith("data:"):
            payload = line[5:].strip()
            if payload and payload != "[DONE]":
                frames.append(json.loads(payload))
    return frames


def _call(url: str, token: str, tool: str, arguments: dict, host: str | None) -> dict:
    init = {"jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": PROTOCOL_VERSION, "capabilities": {},
                       "clientInfo": {"name": "papra-suite-runbook-probe", "version": "0"}}}
    session, frames = _post(url, token, init, None, host)
    if not frames:
        raise SystemExit("initialize returned no payload — the request never reached the server")
    server = frames[0].get("result", {}).get("serverInfo", {})
    print(f"session: {session or '(none offered)'}  server: {server.get('name')} "
          f"{server.get('version')}", flush=True)
    _post(url, token, {"jsonrpc": "2.0", "method": "notifications/initialized"}, session, host)
    _session, frames = _post(
        url, token, {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                     "params": {"name": tool, "arguments": arguments}}, session, host)
    if not frames:
        raise SystemExit(f"{tool} returned no payload")
    return frames[0]


def _report(tool: str, frame: dict) -> int:
    if "error" in frame:
        print(f"{tool}: JSON-RPC error {json.dumps(frame['error'])}")
        return 1
    result = frame.get("result", {})
    if result.get("isError"):
        texts = [c.get("text", "") for c in result.get("content", []) if c.get("type") == "text"]
        print(f"{tool}: refused — {' '.join(texts)}")
        return 1
    payload = result.get("structuredContent")
    if payload is None:
        print(f"{tool}: no structured content — {json.dumps(result)[:400]}")
        return 1
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if tool in ("search_documents", "list_recent_documents"):
        items = payload.get("items", [])
        print(f"\n{tool}: {len(items)} item(s)  partial={payload.get('partial')} "
              f"reason={payload.get('partial_reason')}  scanned={payload.get('scanned')}")
        for item in items:
            print(f"  {item.get('org_id')}  {item.get('document_id')}  {item.get('title')}")
        # The empty answer is the boundary test's pass condition, and the exit code is what a
        # script reads: 0 means the call returned, which is all this probe can promise.
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("tool", help="the tool to call, e.g. search_documents")
    parser.add_argument("arguments", nargs="*", help="name=value; an int-shaped value is an int")
    parser.add_argument("--url", required=True, help="the listener endpoint, e.g. …/mcp")
    parser.add_argument("--token-env", default="PAPRA_MCP_TOKEN",
                        help="name of the environment variable holding the bearer token")
    parser.add_argument("--host", default=None,
                        help="Host header to send, for a loopback call to a listener whose "
                             "allow-list is built from the public URL")
    parser.add_argument("--insecure-host", action="store_true",
                        help="skip certificate hostname checking (loopback calls on the box only)")
    args = parser.parse_args()

    token = os.environ.get(args.token_env, "")
    if not token:
        print(f"no token: set {args.token_env} (it is never an argument, and never echoed)",
              file=sys.stderr)
        return 2
    arguments = {}
    for item in args.arguments:
        key, _, value = item.partition("=")
        arguments[key] = int(value) if value.lstrip("-").isdigit() else value

    context = ssl._create_unverified_context() if args.insecure_host else None  # noqa: S323
    try:
        frame = _call(args.url, token, args.tool, arguments, args.host)
    except urllib.error.HTTPError as error:
        # 401, 403, 421 and 500 all arrive here, and the distinction is the whole point of the
        # run: which gate answered. The body is small and ours; the headers are not printed, because
        # `WWW-Authenticate` is the one place a challenge could carry something unexpected.
        print(f"http {error.code} — the request was refused before a tool ran", file=sys.stderr)
        return 1
    except urllib.error.URLError as error:
        print(f"cannot reach {args.url}: {error.reason}", file=sys.stderr)
        return 1
    return _report(args.tool, frame)


if __name__ == "__main__":
    raise SystemExit(main())
