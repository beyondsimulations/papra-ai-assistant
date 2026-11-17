"""Hook receiver for the Papra webhook probe.

Stdlib only, so the same file runs on the host venv and inside
`python:3.12-slim` with the same command:

    .venv/bin/python docs/superpowers/probes/2026-11-17-kreuzakt-contracts/listener.py

One JSON object per line is appended to `listener.jsonl` next to this file:
`{received_at, path, headers, body}`. `body` is the raw request body parsed with
`json.loads` when it parses, else the decoded text, so an unexpected payload
still lands in the log verbatim instead of being silently swallowed. Every
request answers 200: the receiver is a recorder, not a participant. Retry
policy, `webhookUrlAllowedHostnames` and receiver-robustness work were dropped
with the webhook receiver itself (operator decision, 2026-11-17) — the six-hour
convergence pass covers staleness instead, so nothing here needs to fail on
purpose.

Not ruff-checked: `docs/superpowers/probes/` is excluded in pyproject.toml so a
measured script cannot drift away from the log it produced.
"""

import hashlib
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOG_PATH = os.environ.get(
    "LISTENER_LOG",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "listener.jsonl"),
)
PORT = int(os.environ.get("LISTENER_PORT", "8099"))
_lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # the JSONL is the record; keep stderr quiet
        pass

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("content-length") or 0))
        text = raw.decode("utf-8", "replace")
        try:
            body = json.loads(text)
        except ValueError:
            body = text
        record = {
            "received_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
            + ".%03dZ" % (int(time.time() * 1000) % 1000),
            "path": self.path,
            "headers": dict(self.headers.items()),
            "body": body,
            # `raw_body` is what the signature covers. Re-serialised JSON is not
            # the signed bytes, so a signature check without this field cannot
            # ever be more than a guess.
            "raw_body": text,
            "raw_body_sha256": hashlib.sha256(raw).hexdigest(),
        }
        with _lock, open(LOG_PATH, "a", encoding="utf-8") as log:
            log.write(json.dumps(record, sort_keys=True) + "\n")

        payload = b'{"ok":true}'
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        try:
            with open(LOG_PATH, encoding="utf-8") as log:
                lines = log.readlines()
        except OSError:
            lines = []
        payload = json.dumps({"deliveries": len(lines)}).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else PORT
    print(f"listener on :{port}, log -> {LOG_PATH}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
