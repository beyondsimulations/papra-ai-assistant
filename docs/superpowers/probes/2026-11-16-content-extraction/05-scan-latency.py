"""Probe #14: the production case — a scan with no text layer.

How long does Papra's internal extraction take when Tesseract is needed, and does
it ever finish? That bounds what "wait for the extraction to settle" would have to
mean, and it decides between wait-then-write and write-verify-rewrite.

Also: is there any field on the document that reports extraction state? (fields
listed in full)
"""

import io
import time
import uuid

import fitz
import httpx

BASE = "http://127.0.0.1:12210"


def scanned_pdf() -> bytes:
    """A PDF whose only page is a raster image of text: no text layer at all."""
    text_pdf = fitz.open()
    page = text_pdf.new_page(width=600, height=200)
    page.insert_text((40, 80), "GESCANTER TEXT 4211", fontsize=28)
    png = page.get_pixmap(dpi=200).tobytes("png")
    text_pdf.close()

    out = fitz.open()
    out_page = out.new_page(width=600, height=200)
    out_page.insert_image(out_page.rect, stream=png)
    data = out.tobytes()
    out.close()
    return data


def main() -> None:
    s = uuid.uuid4().hex[:8]
    with httpx.Client(base_url=BASE, timeout=30) as h:
        h.post("/api/auth/sign-up/email", json={
            "email": f"p14-{s}@x.test", "password": "probe-password-123456",
            "name": "P"}).raise_for_status()
        org = h.post("/api/organizations", json={"name": f"P14 {s}"}
                     ).raise_for_status().json()["organization"]["id"]
        tok = h.post("/api/api-keys", json={
            "name": f"k-{s}",
            "permissions": ["organizations:read", "documents:read", "documents:create",
                            "documents:update"],
        }).raise_for_status().json()["token"]

    api = httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {tok}"}, timeout=60)
    body = scanned_pdf() + f"\n% {uuid.uuid4().hex}\n".encode()
    doc = api.post(f"/api/organizations/{org}/documents",
                   files={"file": ("scan.pdf", body, "application/pdf")}
                   ).raise_for_status().json()["document"]
    did = doc["id"]
    print("upload response, all fields:")
    for k, v in sorted(doc.items()):
        print(f"   {k} = {str(v)[:70]!r}")

    t0 = time.monotonic()
    seen = {}
    while time.monotonic() - t0 < 180:
        d = api.get(f"/api/organizations/{org}/documents/{did}").json()["document"]
        content = d.get("content") or ""
        seen[round(time.monotonic() - t0, 1)] = len(content)
        if content:
            print(f"\ncontent appeared at t+{time.monotonic() - t0:.1f}s: {content[:120]!r}")
            break
        time.sleep(1.0)
    else:
        print("\ncontent never appeared within 180 s")
    print("length timeline (first 30 samples):", list(seen.items())[:30])

    print("\nGET document fields after settle:", sorted(d.keys()))
    print("updatedAt vs createdAt:", d["createdAt"], d["updatedAt"])

    # Is the pipeline's write still safe after a long extraction, if we write
    # WHILE it is running?
    marker = f"OURS-{uuid.uuid4().hex[:8]}"
    api.patch(f"/api/organizations/{org}/documents/{did}",
              json={"content": f"our ocr {marker}"}).raise_for_status()
    for tick in (2, 10, 30, 90):
        time.sleep(8)
        c = api.get(f"/api/organizations/{org}/documents/{did}"
                    ).json()["document"].get("content") or ""
        print(f"   +{tick:<3}s marker present: {marker in c} len={len(c)}")


if __name__ == "__main__":
    main()
