"""Probe #12: positive control for the mechanism.

If Papra's own asynchronous content extraction is what overwrites our PATCH, then
the value that replaces ours must be *Papra's extraction of the file*, not blank by
mystery. Upload a text/plain file whose own text is a known sentinel, PATCH our
value immediately, and read back.

  stored == our value      -> no async writer in play for this mime type
  stored == file's text    -> Papra's extraction overwrote us (mechanism proven)
  stored == ''             -> something clears content without extracting
"""

import time

import httpx

BASE = "http://127.0.0.1:12210"
import uuid

TXT = b"THEIR-EXTRACTED-TEXT-9f3c\nsecond line\n"


def main() -> None:
    s = uuid.uuid4().hex[:8]
    with httpx.Client(base_url=BASE, timeout=30) as h:
        h.post("/api/auth/sign-up/email", json={
            "email": f"p12-{s}@x.test", "password": "probe-password-123456",
            "name": "P"}).raise_for_status()
        org = h.post("/api/organizations", json={"name": f"P12 {s}"}
                     ).raise_for_status().json()["organization"]["id"]
        tok = h.post("/api/api-keys", json={
            "name": f"k-{s}",
            "permissions": ["organizations:read", "documents:read", "documents:create",
                            "documents:update"],
        }).raise_for_status().json()["token"]

    api = httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {tok}"}, timeout=30)

    for name, mime, body, patch_first in [
        ("note.txt", "text/plain", TXT, True),
        ("note2.txt", "text/plain", TXT.replace(b"9f3c", b"7a2d"), False),
    ]:
        doc = api.post(f"/api/organizations/{org}/documents",
                       files={"file": (name, body, mime)}).raise_for_status().json()["document"]
        did = doc["id"]
        print(f"\n{name}: upload response content={doc.get('content')!r}")
        if patch_first:
            r = api.patch(f"/api/organizations/{org}/documents/{did}",
                          json={"content": "OURS-PATCHED-VALUE-1"})
            print(f"   PATCH(ours) at +0s -> {r.status_code}")
        for tick in (0.2, 1, 2, 5, 10, 20):
            time.sleep(tick if tick == 0.2 else 0)
            d = api.get(f"/api/organizations/{org}/documents/{did}").json()["document"]
            print(f"   t+{tick:<5} content={d.get('content')!r}")
            time.sleep(max(0, tick - 0.2) / 3)

    # and: does a patch AFTER extraction sticks for the pdf case?
    pdf = (b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
           b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
           b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 200 200]>>endobj\n"
           b"trailer<</Root 1 0 R>>\n%%EOF\n") + f"\n% {uuid.uuid4().hex}\n".encode()
    doc = api.post(f"/api/organizations/{org}/documents",
                   files={"file": ("late.pdf", pdf, "application/pdf")}
                   ).raise_for_status().json()["document"]
    did = doc["id"]
    time.sleep(20)
    mid = api.get(f"/api/organizations/{org}/documents/{did}").json()["document"]
    print(f"\nlate.pdf after 20 s settle: content={mid.get('content')!r}")
    api.patch(f"/api/organizations/{org}/documents/{did}",
              json={"content": "OURS-AFTER-SETTLE-2"}).raise_for_status()
    for tick in (1, 5, 15, 40):
        time.sleep(tick if tick == 1 else 4)
        d = api.get(f"/api/organizations/{org}/documents/{did}").json()["document"]
        print(f"   +{tick}s content={d.get('content')!r}")


if __name__ == "__main__":
    main()
