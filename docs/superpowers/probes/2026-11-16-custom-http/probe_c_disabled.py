"""Probe C: does turning Papra's extraction off make our content write durable?

The image exposes `CONTENT_EXTRACTION_ENABLED`, which was not on the list of options
this decision started with. If it works, single-writer is one environment variable:
no fork, no new listener, no reconciliation loop. This script measures that, and it
runs unchanged against a stack where extraction is ON so the two arms differ in the
one variable that matters.

Arms, run as:
    .venv/bin/python .../probe_c_disabled.py http://127.0.0.1:12211   # extraction off
    .venv/bin/python .../probe_c_disabled.py http://127.0.0.1:12210   # default (control)

Each arm: upload an image-only scan, wait, PATCH our own text in immediately (the
timing that lost the race 5/5 in the earlier paired measurement), then watch the
content column for a minute and search for the token that exists nowhere but there.
"""

import sys
import time
import uuid

import httpx
import fitz

OUR_TOKEN = "QRZ7OURPATCH"

SCAN_LINES = "Rechnung {token}\nDie Wäsche Straße Grüße\nBetrag 12,34 EUR"


def image_only_scan(token: str) -> bytes:
    source = fitz.open()
    page = source.new_page()
    page.insert_text((72, 96), SCAN_LINES.format(token=token), fontsize=14)
    pixmap = page.get_pixmap(dpi=200)
    source.close()
    doc = fitz.open()
    target = doc.new_page(width=612, height=612 * pixmap.height / pixmap.width)
    target.insert_image(target.rect, pixmap=pixmap)
    assert target.get_text().strip() == "", "not image-only: a text layer exists"
    data = doc.tobytes()
    doc.close()
    return data


def provision(client: httpx.Client) -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:8]
    client.post("/api/auth/sign-up/email", json={
        "email": f"probec-{suffix}@papra-suite.test", "password": "probe-password-123",
        "name": "Probe C"}, timeout=30).raise_for_status()
    org = client.post("/api/organizations", json={"name": f"Probe C {suffix}"}, timeout=30)
    org.raise_for_status()
    key = client.post("/api/api-keys", json={
        "name": f"probec-{suffix}",
        "permissions": ["organizations:read", "documents:create", "documents:read",
                        "documents:update"]}, timeout=30)
    key.raise_for_status()
    return org.json()["organization"]["id"], key.json()["token"]


def get_document(api: httpx.Client, org_id: str, document_id: str) -> dict:
    response = api.get(f"/api/organizations/{org_id}/documents/{document_id}", timeout=30)
    response.raise_for_status()
    return response.json()["document"]


def main() -> None:
    base = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:12210"
    our_content = f"UNSOCR {OUR_TOKEN}\nDie Wäsche Straße Grüße\nBetrag 12,34 EUR"

    with httpx.Client(base_url=base) as admin:
        org_id, token = provision(admin)
    api = httpx.Client(base_url=base, headers={"Authorization": f"Bearer {token}"})

    upload = api.post(f"/api/organizations/{org_id}/documents",
                      files={"file": ("scan.pdf", image_only_scan(OUR_TOKEN),
                                      "application/pdf")}, timeout=60)
    upload.raise_for_status()
    document_id = upload.json()["document"]["id"]

    # Patch immediately, with no sleep. This is the timing that matters: it is what
    # the worker actually does (upload returns, OCR runs, PATCH follows within
    # seconds), and it is the timing that lost the race 5/5 in the earlier paired
    # measurement. Patching at +5 s instead would land after Papra's own extraction
    # has already settled (~3 s, measured) and would prove nothing — the first run of
    # this probe did exactly that and both arms appeared to survive.
    before = get_document(api, org_id, document_id).get("content") or ""
    print(f"[{base}] content immediately after upload: {len(before)} chars {before[:60]!r}")

    patched = api.patch(f"/api/organizations/{org_id}/documents/{document_id}",
                        json={"content": our_content}, timeout=30)
    patched.raise_for_status()
    print(f"[{base}] PATCH(content) at +0 s: {patched.status_code}")

    started = time.monotonic()
    timeline = []
    for offset in (0, 2, 5, 10, 20, 30, 45, 60):
        wait = offset - (time.monotonic() - started)
        if wait > 0:
            time.sleep(wait)
        length = len(get_document(api, org_id, document_id).get("content") or "")
        timeline.append((offset, length))
    print(f"[{base}] content length over the next minute: {timeline}")

    survived = timeline[-1][1] == len(our_content)
    print(f"[{base}] ours={len(our_content)} chars; a final length of 63 means Papra's "
          f"extraction landed last and our text is gone")
    listing = api.get(f"/api/organizations/{org_id}/documents",
                      params={"pageIndex": 0, "pageSize": 10, "searchQuery": OUR_TOKEN},
                      timeout=30).json()
    print(f"[{base}] our text still stored: {survived} "
          f"({timeline[-1][1]} chars, ours is {len(our_content)})")
    print(f"[{base}] content search for {OUR_TOKEN}: "
          f"documentsCount={listing.get('documentsCount')}")

    diacritic = api.get(f"/api/organizations/{org_id}/documents",
                        params={"pageIndex": 0, "pageSize": 10, "searchQuery": "Straße"},
                        timeout=30).json()
    print(f"[{base}] content search for 'Straße' (ours wrote it correctly): "
          f"documentsCount={diacritic.get('documentsCount')}")
    api.close()


if __name__ == "__main__":
    main()
