"""Unit tests for the integration readiness probe (no Docker required)."""

import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from tests.integration.conftest import _probe_readiness


def test_probe_retries_on_503_until_200():
    """A server answering 503 while booting must not fail the probe outright;
    the probe retries on any response until the window expires."""
    start = time.monotonic()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if time.monotonic() - start < 2.0:
                self.send_response(503)
                self.end_headers()
            else:
                self.send_response(200)
                self.end_headers()

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{port}"
        assert _probe_readiness(url, readiness_timeout_s=10.0, poll_interval_s=0.1)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)
