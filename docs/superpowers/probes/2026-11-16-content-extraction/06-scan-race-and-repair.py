"""Probe #15: bound the damage and test the repair, on a realistic scan.

1. Extraction latency for an 8-page 200-dpi scan (the production shape).
2. Pipeline-shaped race: upload -> PATCH(content) right away (a fast OCR answer)
   -> is it clobbered?
3. Repair: after the extraction has clearly finished, PATCH(content) again ->
   does it survive to +90 s? One pass or repeated overwrites?
"""

import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import fitz
import httpx

BASE = "http://127.0.0.1:12210"
_state: dict[str, str] = {}


def org_and_key() -> tuple[str, str]:
    if not _state:
        s = uuid.uuid4().hex[:8]
        with httpx.Client(base_url=BASE, timeout=60) as h:
            h.post("/api/auth/sign-up/email", json={
                "email": f"p15-{s}@x.test", "password": "probe-password-123456",
                "name": "P"}).raise_for_status()
            _state["org"] = h.post("/api/organizations", json={"name": f"P15 {s}"}
                                   ).raise_for_status().json()["organization"]["id"]
            _state["tok"] = h.post("/api/api-keys", json={
                "name": f"k-{s}",
                "permissions": ["organizations:read", "documents:read",
                                "documents:create", "documents:update"],
            }).raise_for_status().json()["token"]
    return _state["org"], _state["tok"]


def scan_pdf(pages: int = 8, dpi: int = 200) -> bytes:
    seed = fitz.open()
    p = seed.new_page(width=800, height=1000)
    for y in range(80, 950, 60):
        p.insert_text((60, y), f"BELEGZEILE {y} BETRAG 1.234,56 EUR KUNDE MEIER",
                      fontsize=15)
    png = p.get_pixmap(dpi=dpi).tobytes("png")
    seed.close()

    out = fitz.open()
    for _ in range(pages):
        page = out.new_page(width=800, height=1000)
        page.insert_image(page.rect, stream=png)
    data = out.tobytes()
    out.close()
    return data


def settle_seconds(api: httpx.Client, org: str, did: str, limit: float) -> tuple[float, int]:
    """Seconds until the stored content stopped changing, and its final length."""
    t0 = time.monotonic()
    last_len, last_change = -1, t0
    while time.monotonic() - t0 < limit:
        c = api.get(f"/api/organizations/{org}/documents/{did}").json()["document"].get("content") or ""
        if len(c) != last_len:
            last_len, last_change = len(c), time.monotonic()
            if c:
                # keep watching one more second to be sure it is done
                time.sleep(1.0)
                again = api.get(f"/api/organizations/{org}/documents/{did}"
                                ).json()["document"].get("content") or ""
                if len(again) == len(c):
                    return time.monotonic() - t0, len(c)
        time.sleep(0.5)
    return time.monotonic() - t0, last_len


def main() -> None:
    org, tok = org_and_key()
    body = scan_pdf()
    print(f"scan fixture size: {len(body)/1024/1024:.1f} MiB, 8 pages at 200 dpi")

    api = httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {tok}"}, timeout=120)
    doc = api.post(f"/api/organizations/{org}/documents",
                   files={"file": (f"big-{uuid.uuid4().hex[:6]}.pdf", body, "application/pdf")}
                   ).raise_for_status().json()["document"]
    did = doc["id"]
    waited, length = settle_seconds(api, org, did, limit=180)
    print(f"1. extraction settled after {waited:.1f}s, content length {length}")

    print("\n2. race: upload then PATCH(content) immediately (n=4)")

    def race(i: int) -> str:
        token = f"RACE{i}{uuid.uuid4().hex[:6]}"
        with httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {tok}"},
                          timeout=120) as a:
            d = a.post(f"/api/organizations/{org}/documents",
                       files={"file": (f"r{i}-{uuid.uuid4().hex[:6]}.pdf",
                                       body + f"\n% {uuid.uuid4().hex}\n".encode(),
                                       "application/pdf")}
                       ).raise_for_status().json()["document"]
            a.patch(f"/api/organizations/{org}/documents/{d['id']}",
                    json={"content": f"# OCR\n\n{token}"}).raise_for_status()
            time.sleep(20)
            c = a.get(f"/api/organizations/{org}/documents/{d['id']}"
                      ).json()["document"].get("content") or ""
            ours = token in c
            # 3. repair once, after extraction has had 20 s
            a.patch(f"/api/organizations/{org}/documents/{d['id']}",
                    json={"content": f"# OCR\n\n{token}"}).raise_for_status()
            time.sleep(45)
            c2 = a.get(f"/api/organizations/{org}/documents/{d['id']}"
                       ).json()["document"].get("content") or ""
            hits = a.get(f"/api/organizations/{org}/documents",
                         params={"pageIndex": 0, "pageSize": 20,
                                 "searchQuery": token}).json()["documents"]
            return (f"   doc{i}: after 20s ours={ours} | after repair+45s "
                    f"ours={'token' in c2 and token in c2} searchable="
                    f"{any(h['id'] == d['id'] for h in hits)} len={len(c2)}")

    with ThreadPoolExecutor(max_workers=4) as pool:
        for text in pool.map(race, range(4)):
            print(text)


if __name__ == "__main__":
    main()
