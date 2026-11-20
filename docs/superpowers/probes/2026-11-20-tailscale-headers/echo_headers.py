"""Print the headers a `tailscale serve` proxy actually forwards to a loopback listener.

Why this exists: `papra-suite serve` answers `421` to a `Host` its allow-list does not name and `403`
to an `Origin` it does not name, and that allow-list is built from `PAPRA_MCP_URL` at startup. Both
values are set from what the proxy is believed to send, and nothing in this repository can observe
the proxy — the listener tests bind a socket on localhost, where the `Host` is the one the test
chose. So the operator has to look once, on the deployment, at what arrives after the proxy.

What it does: listens on `127.0.0.1:8765` — the same interface and port the MCP listener will later
take — and prints, for every request, the five headers the deployment depends on and then every
other header that arrived. It answers `200 ok` and writes nothing. **Stdlib only**, so it runs in
whatever python3 is on the box without an install, and it cannot itself change what is measured.

Read the output as the allow-list's input, not as a verdict: what the header *is* decides whether
`PAPRA_MCP_URL` covers it. If it does not, the fix is the environment value, never a widened list —
a `Host` allow-list that accepts anything is a 421 gate that is not a gate.

Run it, drive one request through the proxy from another machine on the tailnet, then Ctrl-C:

    python3 echo_headers.py
    curl -sS -i https://<the-name-ts.net>/measure

Both halves matter: the request has to arrive **through the proxy** for the headers to be the ones
the MCP listener will see. A request to `http://127.0.0.1:8765/` on the box itself measures the
direct path, which is not the path in question.
"""

import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOST = "127.0.0.1"
PORT = 8765

#: The headers the listener's transport check and proxy handling read. `Host` and `Origin` decide
#: 421 and 403; the `X-Forwarded-*` trio is what uvicorn's proxy-header handling consumes, and this
#: build trusts it only from loopback peers, which is why the source address is printed too.
INTERESTING = ("host", "origin", "x-forwarded-for", "x-forwarded-proto", "x-forwarded-host")


class Echo(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _report(self) -> None:
        peer = f"{self.client_address[0]}:{self.client_address[1]}"
        print(f"--- {self.command} {self.path}  from {peer}", flush=True)
        headers = dict(self.headers.items())
        for name in INTERESTING:
            value = self.headers.get(name)
            print(f"    {name:<20} {value if value is not None else '(absent)'}", flush=True)
        extra = sorted(k for k in headers if k.lower() not in INTERESTING)
        if extra:
            print(f"    other headers ({len(extra)}):", flush=True)
            for name in extra:
                print(f"      {name}: {self.headers.get(name)}", flush=True)
        else:
            print("    other headers: none", flush=True)

    def _answer(self) -> None:
        self.send_response(200)
        body = b"ok\n"
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler's name
        self._report()
        self._answer()

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler's name
        # Drain the body so a keep-alive client does not see a broken pipe: the MCP transport
        # POSTs JSON, and an operator may point a real client at this by mistake.
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)
        self._report()
        self._answer()

    def log_message(self, fmt: str, *args: object) -> None:
        # The default writes one line per request to stderr in addition to ours; the interesting
        # bytes are already on stdout, and two formats for one request only makes the table harder
        # to read.
        pass


def main() -> int:
    print(f"listening on http://{HOST}:{PORT} — Ctrl-C to stop", flush=True)
    print(f"printing: {', '.join(INTERESTING)}, then everything else that arrived", flush=True)
    try:
        with ThreadingHTTPServer((HOST, PORT), Echo) as server:
            server.serve_forever()
    except OSError as error:
        # The everyday case is `[Errno 98] Address already in use`: the MCP listener, or whatever
        # else already owns the port. Naming it beats a traceback, because the operator's next
        # question is "then what did my request reach?"
        print(f"cannot bind {HOST}:{PORT}: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("stopped", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
