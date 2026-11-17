"""Probe B2: does Papra ever retry a failed extraction call, and does a late answer count?

probe_b_custom_http.py observed ~25 s and reported "no retry, empty permanently". That
overstates a 25-second window: it shows no retry *within 25 s*, and it cannot rule out a
late write from a request still in flight — which is exactly the assumption a bounded-window
sweeper would depend on. This run watches for three minutes and answers both questions:
whether a second request ever arrives, and whether a response that lands after the 5 s
client timeout is ever stored.

Run with the custom-http container up (see README.md in this directory):
  .venv/bin/python docs/superpowers/probes/2026-11-16-custom-http/probe_b2_retry.py
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
WINDOW_S = 180.0

STATE = {"mode": "error-500", "requests": []}


class Catcher(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        body = self.rfile.read(int(self.headers.get("content-length", "0")))
        STATE["requests"].append({"at": time.monotonic(), "bytes": len(body),
                                  "filename": self._filename(body)})
        mode = STATE["mode"]
        if mode == "error-500":
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"extraction failed")
            return
        # "hang": answer only after the client-side timeout has fired, so a late
        # response is visible as a late write (or its absence) rather than as a failure.
        time.sleep(12)
        payload = b"LATE-ANSWER-11"
        self.send_response(200)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    @staticmethod
    def _filename(body: bytes) -> str:
        try:
            document = json.loads(body).get("document", {})
            decoded = base64.b64decode(document.get("content", ""))
            return f"json:{document.get('filename')}:{len(decoded)}b"
        except (ValueError, TypeError):
            head = body[:400]
            marker = b'filename="'
            start = head.find(marker)
            if start < 0:
                return "raw"
            end = head.find(b'"', start + len(marker))
            return f"multipart:{head[start + len(marker):end].decode(errors='replace')}"

    def log_message(self, *args):
        pass


def image_only_pdf() -> bytes:
    source = fitz.open()
    page = source.new_page()
    page.insert_text((72, 96), "Rechnung QRZ7RETRYPROBE", fontsize=14)
    pixmap = page.get_pixmap(dpi=150)
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
        "email": f"probeb2-{suffix}@papra-suite.test", "password": "probe-password-123",
        "name": "Probe B2"}, timeout=30).raise_for_status()
    org = client.post("/api/organizations", json={"name": f"Probe B2 {suffix}"}, timeout=30)
    org.raise_for_status()
    key = client.post("/api/api-keys", json={
        "name": f"probeb2-{suffix}",
        "permissions": ["organizations:read", "documents:create", "documents:read",
                        "documents:update"]}, timeout=30)
    key.raise_for_status()
    return org.json()["organization"]["id"], key.json()["token"]


def observe(api: httpx.Client, org_id: str, document_id: str, label: str) -> None:
    started = time.monotonic()
    seen = set()
    for offset in (0, 5, 15, 30, 60, 120, WINDOW_S):
        wait = offset - (time.monotonic() - started)
        if wait > 0:
            time.sleep(wait)
        content = api.get(f"/api/organizations/{org_id}/documents/{document_id}",
                          timeout=30).json()["document"].get("content") or ""
        requests = [r for r in STATE["requests"] if r["at"] >= started]
        key = (len(requests), len(content))
        if key not in seen:
            seen.add(key)
            print(f"  [{label}] t+{offset:6.0f}s  extraction-requests={len(requests)} "
                  f"content={content[:24]!r} ({len(content)} chars)")
    # Always state the end of the window. Printing only changes made the first run look as
    # though it had stopped at t+5s, which left the later half of the window unevidenced
    # for the case that needed it most.
    final = [r for r in STATE["requests"] if r["at"] >= started]
    content = api.get(f"/api/organizations/{org_id}/documents/{document_id}",
                      timeout=30).json()["document"].get("content") or ""
    print(f"  [{label}] end of {WINDOW_S:.0f}s window: extraction-requests={len(final)} "
          f"content={content[:40]!r} ({len(content)} chars)")


def main() -> None:
    server = HTTPServer(("0.0.0.0", CATCH_PORT), Catcher)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    with httpx.Client(base_url=BASE) as admin:
        org_id, token = provision(admin)
    api = httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {token}"})
    try:
        for mode, label in (("error-500", "500"), ("hang", "answer-after-timeout")):
            STATE["mode"] = mode
            STATE["requests"].clear()
            response = api.post(f"/api/organizations/{org_id}/documents",
                                files={"file": (f"{mode}.pdf", image_only_pdf(),
                                                "application/pdf")}, timeout=120)
            response.raise_for_status()
            print(f"\n=== {label} (upload returned {response.status_code}) ===")
            observe(api, org_id, response.json()["document"]["id"], label)
        print(f"\ncase 2 received {len(STATE['requests'])} extraction request(s) "
              f"across its full {WINDOW_S:.0f}s window")
    finally:
        api.close()
        server.shutdown()


if __name__ == "__main__":
    main()
