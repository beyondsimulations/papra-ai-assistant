"""Probe C2: the write race, measured with exact text and a token Papra cannot see.

Corrects two confounds in probe_c_disabled.py, both raised after review of that run:

1. It compared content *lengths*. Equal length is not equal text, and 60 vs 63 chars is a
   coincidence of these fixtures, not a test.
2. It put the same token in the scanned image and in the text we PATCH, so a content-search
   hit proved nothing about whose text was stored — Papra's own OCR reads the token off the
   page. Measured, in fact: in the default arm the token search hit while our text was gone.

Here the image carries QRZ7IMAGEONLY and our PATCH carries QRZ7PATCHONLY, which appears on
no pixel of the page. A search hit on QRZ7PATCHONLY therefore can only mean our text is what
Papra is indexing. Equality is checked against the exact string.

Run:  .venv/bin/python .../probe_c2_exact.py http://127.0.0.1:12211   # extraction off
      .venv/bin/python .../probe_c2_exact.py http://127.0.0.1:12210   # default (control)
"""

import sys
import time
import uuid

import httpx
import fitz

IMAGE_TOKEN = "QRZ7IMAGEONLY"
PATCH_TOKEN = "QRZ7PATCHONLY"
OUR_CONTENT = f"OURTEXT {PATCH_TOKEN}\nDie Wäsche Straße Grüße\nBetrag 12,34 EUR"


def image_only_scan() -> bytes:
    source = fitz.open()
    page = source.new_page()
    page.insert_text((72, 96), f"Rechnung {IMAGE_TOKEN}\nDie Wäsche Straße\nBetrag 12,34 EUR",
                     fontsize=14)
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
        "email": f"probec2-{suffix}@papra-suite.test", "password": "probe-password-123",
        "name": "Probe C2"}, timeout=30).raise_for_status()
    org = client.post("/api/organizations", json={"name": f"Probe C2 {suffix}"}, timeout=30)
    org.raise_for_status()
    key = client.post("/api/api-keys", json={
        "name": f"probec2-{suffix}",
        "permissions": ["organizations:read", "documents:create", "documents:read",
                        "documents:update"]}, timeout=30)
    key.raise_for_status()
    return org.json()["organization"]["id"], key.json()["token"]


def content_of(api: httpx.Client, org_id: str, document_id: str) -> str:
    response = api.get(f"/api/organizations/{org_id}/documents/{document_id}", timeout=30)
    response.raise_for_status()
    return response.json()["document"].get("content") or ""


def search_hit(api: httpx.Client, org_id: str, document_id: str, query: str) -> str:
    """Return 'hit' / 'miss' for this document specifically.

    Asserting on the document id rather than a count matters: an org-wide count answers a
    different question than 'is this document findable'.
    """
    response = api.get(f"/api/organizations/{org_id}/documents",
                       params={"pageIndex": 0, "pageSize": 25, "searchQuery": query},
                       timeout=30)
    response.raise_for_status()
    ids = [document.get("id") for document in response.json().get("documents", [])]
    return "hit" if document_id in ids else "miss"


def main() -> None:
    base = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:12210"
    with httpx.Client(base_url=base) as admin:
        org_id, token = provision(admin)
    api = httpx.Client(base_url=base, headers={"Authorization": f"Bearer {token}"})

    upload = api.post(f"/api/organizations/{org_id}/documents",
                      files={"file": ("scan.pdf", image_only_scan(), "application/pdf")},
                      timeout=60)
    upload.raise_for_status()
    document_id = upload.json()["document"]["id"]

    api.patch(f"/api/organizations/{org_id}/documents/{document_id}",
              json={"content": OUR_CONTENT}, timeout=30).raise_for_status()

    started = time.monotonic()
    samples = []
    for offset in (0, 2, 5, 10, 20, 30, 45, 60, 90):
        wait = offset - (time.monotonic() - started)
        if wait > 0:
            time.sleep(wait)
        samples.append((offset, content_of(api, org_id, document_id) == OUR_CONTENT))
    print(f"[{base}] PATCH at +0 s; (offset, exact-text-equal-to-ours): {samples}")

    papra_text = content_of(api, org_id, document_id)
    print(f"[{base}] final content: {papra_text[:80]!r}")
    print(f"[{base}] our text stored exactly : {papra_text == OUR_CONTENT}")
    print(f"[{base}] papra's own text visible: {IMAGE_TOKEN in papra_text}")
    print(f"[{base}] search {PATCH_TOKEN} (only our text has it)  : "
          f"{search_hit(api, org_id, document_id, PATCH_TOKEN)}")
    print(f"[{base}] search {IMAGE_TOKEN} (only the image has it)  : "
          f"{search_hit(api, org_id, document_id, IMAGE_TOKEN)}")
    print(f"[{base}] search 'Straße' (ours spells it correctly)    : "
          f"{search_hit(api, org_id, document_id, 'Straße')}")
    print(f"[{base}] search 'Wäsche' (umlaut folds at query time)  : "
          f"{search_hit(api, org_id, document_id, 'Wäsche')}")
    api.close()


if __name__ == "__main__":
    main()
