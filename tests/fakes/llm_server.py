import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class FakeLLMServer:
    """Threaded stdlib HTTP server impersonating an OpenAI-compatible endpoint.
    Route-aware: serves POST /v1/chat/completions ONLY, from a scripted response
    list (repeating the last), capturing every request body. Any other path is a
    404 recorded in off_route_paths and consumes NO scripted response and NO
    entry in requests — so a client posting the wrong route fails visibly
    instead of silently burning the script. Failure injection: append an
    (status_code, body) tuple instead of a string."""

    def __init__(self) -> None:
        self._responses: list = []
        self._requests: list[dict] = []
        self._off_route_paths: list[str] = []
        self._lock = threading.Lock()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._make_handler())
        self._thread: threading.Thread | None = None

    def _make_handler(self):
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length)
                if self.path != "/v1/chat/completions":
                    # checked before parsing/recording: an off-route request is
                    # not a completion attempt and must not touch the script
                    with outer._lock:
                        outer._off_route_paths.append(self.path)
                    payload = json.dumps({"error": "not found"}).encode()
                    self.send_response(404)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                body = json.loads(raw)
                with outer._lock:
                    outer._requests.append(body)
                    response = outer._responses[min(len(outer._requests) - 1,
                                                     len(outer._responses) - 1)] \
                        if outer._responses else "ok"
                if isinstance(response, tuple):
                    status, payload = response
                else:
                    status, payload = 200, {"choices": [{"message": {"content": response}}]}
                data = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        return Handler

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    @property
    def responses(self) -> list:
        return self._responses

    @property
    def requests(self) -> list[dict]:
        with self._lock:
            return list(self._requests)

    @property
    def off_route_paths(self) -> list[str]:
        with self._lock:
            return list(self._off_route_paths)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
