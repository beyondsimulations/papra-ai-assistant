"""Probe B: the custom-http extraction contract, measured from the wire.

Disabling Papra's extraction turned out to remove the write race outright
(probe_c_disabled.py), which makes custom-http the fallback rather than the primary
option. It is still worth pinning down, because it is the one design where Papra owns
the write and *our* model still produces the text — no PATCH, so no second writer, and
extraction keeps working for any document that reaches Papra by another route.

Everything here is read off the actual request Papra sends, not off the config
registry's documentation strings: the same registry whose env names I first
mis-transcribed from a substring grep, so the strings are exactly what is not trustable
until a packet says otherwise.

Container (started by hand, see docs/superpowers/probes/2026-11-16-custom-http/README.md):
    CONTENT_EXTRACTION_STRATEGY=custom-http
    CONTENT_EXTRACTION_CUSTOM_HTTP_URL=http://host.docker.internal:12399/extract
    CONTENT_EXTRACTION_CUSTOM_HTTP_RESPONSE_FORMAT=text
    CONTENT_EXTRACTION_CUSTOM_HTTP_UPLOAD_FORMAT=form-data
    CONTENT_EXTRACTION_CUSTOM_HTTP_REQUEST_TIMEOUT_MS=5000

Run:  .venv/bin/python docs/superpowers/probes/2026-11-16-custom-http/probe_b_custom_http.py
"""

import base64
import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
import fitz

BASE = "http://127.0.0.1:12212"
CATCH_PORT = 12399

STATE = {"mode": "text-ok", "requests": []}


class Catcher(BaseHTTPRequestHandler):
    """Records what Papra sends and answers according to STATE['mode'].

    The three modes are the three outcomes the design has to survive: we answer with
    text, we fail, and we are too slow (a busy local VLM is the realistic version of
    that one, not a contrivance).
    """

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("content-length", "0"))
        body = self.rfile.read(length)
        STATE["requests"].append({
            "at": time.time(),
            "method": self.command,
            "path": self.path,
            "content_type": self.headers.get("content-type"),
            "headers": {k.lower(): v for k, v in self.headers.items()
                        if k.lower() not in ("content-length",)},
            "bytes": len(body),
            "preview": self._describe(body),
        })

        mode = STATE["mode"]
        if mode == "error-500":
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"extraction failed")
            return
        if mode == "hang":
            time.sleep(12)          # longer than the 5 s timeout in the container
            self.send_response(200)
            self.end_headers()
            return

        text = "CUSTOMHTTP-RESULT-7 extracted-by-our-model"
        payload = text.encode()
        self.send_response(200)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    @staticmethod
    def _describe(body: bytes) -> str:
        try:
            parsed = json.loads(body)
            document = parsed.get("document", {})
            content = document.get("content", "")
            decoded = base64.b64decode(content) if content else b""
            return (f"json keys={sorted(parsed)} document_keys={sorted(document)} "
                    f"filename={document.get('filename')!r} type={document.get('type')!r} "
                    f"base64-decodes-to={len(decoded)} bytes "
                    f"pdf-magic={decoded[:5]!r}")
        except (ValueError, TypeError):
            return f"raw {body[:120]!r}"

    def log_message(self, *args):  # silence the default stderr chatter
        pass


def start_catcher() -> HTTPServer:
    server = HTTPServer(("0.0.0.0", CATCH_PORT), Catcher)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def image_only_pdf() -> bytes:
    source = fitz.open()
    page = source.new_page()
    page.insert_text((72, 96), "Rechnung QRZ7CUSTOMHTTP\nBetrag 12,34 EUR", fontsize=14)
    pixmap = page.get_pixmap(dpi=200)
    source.close()
    doc = fitz.open()
    target = doc.new_page(width=612, height=612 * pixmap.height / pixmap.width)
    target.insert_image(target.rect, pixmap=pixmap)
    data = doc.tobytes()
    doc.close()
    return data


def provision(client: httpx.Client) -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:8]
    client.post("/api/auth/sign-up/email", json={
        "email": f"probeb-{suffix}@papra-suite.test", "password": "probe-password-123",
        "name": "Probe B"}, timeout=30).raise_for_status()
    org = client.post("/api/organizations", json={"name": f"Probe B {suffix}"}, timeout=30)
    org.raise_for_status()
    key = client.post("/api/api-keys", json={
        "name": f"probeb-{suffix}",
        "permissions": ["organizations:read", "documents:create", "documents:read",
                        "documents:update"]}, timeout=30)
    key.raise_for_status()
    return org.json()["organization"]["id"], key.json()["token"]


def settle(api: httpx.Client, org_id: str, document_id: str, window_s: float = 25.0):
    started = time.monotonic()
    previous, stable_at = "", None
    while time.monotonic() - started < window_s:
        current = api.get(f"/api/organizations/{org_id}/documents/{document_id}",
                          timeout=30).json()["document"].get("content") or ""
        if current and current == previous:
            if stable_at is None:
                stable_at = time.monotonic()
            if time.monotonic() - stable_at >= 2.0:
                return time.monotonic() - started, current
        else:
            stable_at = None
        previous = current
        time.sleep(0.5)
    return float("nan"), previous


def run_case(api, org_id, *, mode: str, label: str) -> None:
    STATE["mode"] = mode
    STATE["requests"].clear()

    started = time.monotonic()
    response = api.post(f"/api/organizations/{org_id}/documents",
                        files={"file": (f"{mode}.pdf", image_only_pdf(),
                                        "application/pdf")},
                        timeout=120)
    elapsed_upload = time.monotonic() - started
    print(f"\n=== mode={mode} ({label}) ===")
    print(f"  upload response      : {response.status_code} in {elapsed_upload:.1f}s")
    if response.status_code >= 400:
        print(f"  upload body          : {response.text[:200]}")
        return
    document_id = response.json()["document"]["id"]
    elapsed, content = settle(api, org_id, document_id)
    print(f"  requests we received : {len(STATE['requests'])}")
    for request in STATE["requests"]:
        print(f"    {request['method']} {request['path']} "
              f"content-type={str(request['content_type'])[:60]!r} "
              f"bytes={request['bytes']}")
        print(f"      preview: {request['preview'][:220]}")
        print(f"      headers: {sorted(k for k in request['headers'])}")
    print(f"  final content ({len(content)} chars, settled {elapsed:.1f}s): {content[:120]!r}")


def main() -> None:
    server = start_catcher()
    with httpx.Client(base_url=BASE) as admin:
        org_id, token = provision(admin)
    api = httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {token}"})
    try:
        run_case(api, org_id, mode="text-ok", label="we answer with plain text")
        run_case(api, org_id, mode="error-500", label="our endpoint fails")
        run_case(api, org_id, mode="hang", label="we exceed the 5 s request timeout")
    finally:
        api.close()
        server.shutdown()


if __name__ == "__main__":
    main()
