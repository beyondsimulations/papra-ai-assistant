"""Probe A: what does Papra's DEFAULT extraction produce, with no input from us?

Two documents are uploaded and nothing else is done to them — no PATCH, no content
write from this pipeline — so every byte that ends up in the `content` column was
written by Papra. A text PDF and an image-only scan, each carrying a token that
appears nowhere in the filename, answer the question the earlier probes could not:
the "scans" measured before were PyMuPDF-rendered and therefore had a text layer,
so they proved nothing about image-only input.

Run:  .venv/bin/python docs/superpowers/probes/2026-11-16-custom-http/probe_a_defaults.py
"""

import time
import uuid

import httpx
import fitz

BASE = "http://127.0.0.1:12210"

SCAN_TOKEN = "QRZ7SCANMARK"
TEXT_TOKEN = "QRZ7TEXTMARK"


def make_text_pdf() -> bytes:
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 96), f"Rechnung {TEXT_TOKEN}\nBitte uberweisen Sie 47,11 EUR.",
                     fontsize=14)
    data = doc.tobytes()
    doc.close()
    return data


def make_image_only_pdf() -> bytes:
    """Text drawn into a bitmap and inserted as an image: a real scan, no text layer.

    The check at the end is the point. If `get_text()` returns anything, this is a
    text-layer PDF wearing a costume, and every conclusion drawn from it about
    image-only extraction would be false — which is exactly the mistake this probe
    exists to correct.
    """
    source = fitz.open()
    page = source.new_page()
    page.insert_text((72, 96), f"Rechnung {SCAN_TOKEN}\nBitte uberweisen Sie 47,11 EUR.",
                     fontsize=14)
    pixmap = page.get_pixmap(dpi=200)
    source.close()

    doc = fitz.open()
    target = doc.new_page(width=pixmap.width * 72 / 200, height=pixmap.height * 72 / 200)
    target.insert_image(target.rect, pixmap=pixmap)
    assert target.get_text().strip() == "", "not image-only: a text layer exists"
    data = doc.tobytes()
    doc.close()
    return data


def sign_up(client: httpx.Client) -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:8]
    client.post("/api/auth/sign-up/email", json={
        "email": f"probea-{suffix}@papra-suite.test", "password": "probe-password-123",
        "name": "Probe A"}, timeout=30).raise_for_status()
    org = client.post("/api/organizations", json={"name": f"Probe A {suffix}"},
                      timeout=30)
    org.raise_for_status()
    key = client.post("/api/api-keys", json={
        "name": f"probea-{suffix}",
        "permissions": ["organizations:read", "documents:create", "documents:read",
                        "documents:update"]}, timeout=30)
    key.raise_for_status()
    return org.json()["organization"]["id"], key.json()["token"]


def upload(client: httpx.Client, org_id: str, name: str, data: bytes) -> str:
    response = client.post(f"/api/organizations/{org_id}/documents",
                           files={"file": (name, data, "application/pdf")}, timeout=60)
    response.raise_for_status()
    return response.json()["document"]["id"]


def content_of(client: httpx.Client, org_id: str, document_id: str) -> str:
    response = client.get(f"/api/organizations/{org_id}/documents/{document_id}",
                          timeout=30)
    response.raise_for_status()
    return response.json()["document"].get("content") or ""


def watch_extraction(client: httpx.Client, org_id: str, document_id: str, *, window_s: float,
                     poll_s: float = 0.5) -> tuple[float, str]:
    """Return (seconds until the content stopped changing, final content).

    'Stable for two consecutive reads' is used as a settle signal and nothing more:
    it is not an extraction-complete signal (the characterisation spec's addendum
    says why), so a value that is still growing after the window is reported as
    unsettled rather than treated as final.
    """
    deadline = time.monotonic() + window_s
    previous = ""
    stable_since = None
    while time.monotonic() < deadline:
        current = content_of(client, org_id, document_id)
        if current and current == previous:
            if stable_since is None:
                stable_since = time.monotonic()
            if time.monotonic() - stable_since >= 2.0:
                return time.monotonic() - (deadline - window_s), current
        else:
            stable_since = None
        previous = current
        time.sleep(poll_s)
    return float("nan"), previous


def search(client: httpx.Client, org_id: str, token: str):
    """Same path and parameter names the pipeline's own client uses, so the probe
    measures the search Papra serves to this product rather than another route."""
    response = client.get(
        f"/api/organizations/{org_id}/documents",
        params={"pageIndex": 0, "pageSize": 10, "searchQuery": token}, timeout=30)
    response.raise_for_status()
    payload = response.json()
    return payload.get("documentsCount"), [
        d.get("name") for d in payload.get("documents", [])]


def main() -> None:
    fixtures = (("scan-image-only.pdf", make_image_only_pdf(), SCAN_TOKEN),
                ("text-layer.pdf", make_text_pdf(), TEXT_TOKEN))
    # Two clients, deliberately. Provisioning (sign-up, org, api key) is session
    # authenticated and leaves a better-auth cookie on the client; API-key calls made
    # from a client that ALSO carries that cookie come back 401 auth.unauthorized
    # rather than being served by either credential. Measured, not assumed: the same
    # key uploads fine from a cookie-free client and is refused from one that holds
    # the session cookie. PapraClient never sees a cookie, which is why the product
    # path never hits this.
    with httpx.Client(base_url=BASE) as admin:
        org_id, token = sign_up(admin)

    with httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {token}"}) as client:
        for name, data, token_text in fixtures:
            started = time.monotonic()
            document_id = upload(client, org_id, name, data)
            settle, content = watch_extraction(client, org_id, document_id, window_s=30.0)
            count, names = search(client, org_id, token_text)
            print(f"\n=== {name} ({len(data)} bytes) ===")
            print(f"  papra content length     : {len(content)}")
            print(f"  settled after            : {settle:.1f}s "
                  f"(upload+{time.monotonic() - started:.1f}s total)")
            print(f"  token in content         : {token_text in content}")
            print(f"  content search '{token_text}': documentsCount={count} -> {names}")
            print(f"  content[:160]            : {content[:160]!r}")


if __name__ == "__main__":
    main()
