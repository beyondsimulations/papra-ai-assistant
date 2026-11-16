"""Probe #10: paired confirmation — is a content write near upload time destroyed?

Arm A (n=5): upload -> PATCH(content) immediately.
Arm B (n=5): upload -> sleep 10 -> PATCH(content).
The search token appears ONLY in the content, never in the filename (spike #9's
filename token made every search a name match, which confounded it).

Per document, at t+0.2 / 2 / 10 / 30 / 60 after the write:
  stored content length, and whether a content-token search returns the doc.
Reports the arm-level tally, which is the claim being tested.
"""

import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx

BASE = "http://127.0.0.1:12210"
PDF = (b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
       b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
       b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 200 200]>>endobj\n"
       b"trailer<</Root 1 0 R>>\n%%EOF\n")
N = 5
TICKS = (0.2, 2, 10, 30, 60)
_state: dict[str, str] = {}
_lock = threading.Lock()


def org_and_key() -> tuple[str, str]:
    with _lock:
        if not _state:
            s = uuid.uuid4().hex[:8]
            with httpx.Client(base_url=BASE, timeout=30) as h:
                h.post("/api/auth/sign-up/email", json={
                    "email": f"p10-{s}@x.test", "password": "probe-password-123456",
                    "name": "P"}).raise_for_status()
                _state["org"] = h.post("/api/organizations", json={"name": f"P10 {s}"}
                                       ).raise_for_status().json()["organization"]["id"]
                _state["tok"] = h.post("/api/api-keys", json={
                    "name": f"k-{s}",
                    "permissions": ["organizations:read", "documents:read",
                                    "documents:create", "documents:update"],
                }).raise_for_status().json()["token"]
    return _state["org"], _state["tok"]


def run(arm: str, index: int) -> str:
    org, tok = org_and_key()
    token = f"ctok{arm}{index}{uuid.uuid4().hex[:6]}"          # content only
    filename = f"invoice-{arm}{index}-{uuid.uuid4().hex[:6]}.pdf"  # never the token
    rows: list[str] = [f"{arm}{index} token={token} file={filename}"]
    with httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {tok}"},
                      timeout=30) as api:
        body = PDF + f"\n% {uuid.uuid4().hex}\n".encode()
        doc = api.post(f"/api/organizations/{org}/documents",
                       files={"file": (filename, body, "application/pdf")}
                       ).raise_for_status().json()["document"]
        did = doc["id"]
        uploaded = time.monotonic()
        if arm == "B":
            time.sleep(10)
        patched = time.monotonic()
        r = api.patch(f"/api/organizations/{org}/documents/{did}",
                      json={"content": f"alpha {token} omega"})
        rows.append(f"    upload->patch = {patched - uploaded:.2f} s, status {r.status_code}")
        survived = True
        for tick in TICKS:
            target = patched + tick
            while time.monotonic() < target:
                time.sleep(0.05)
            d = api.get(f"/api/organizations/{org}/documents/{did}").json()["document"]
            hits = api.get(f"/api/organizations/{org}/documents",
                           params={"pageIndex": 0, "pageSize": 20,
                                   "searchQuery": token}).json()["documents"]
            stored = d.get("content") or ""
            found = any(h["id"] == did for h in hits)
            if not stored:
                survived = False
            rows.append(f"    t+{tick:<5} content_len={len(stored):<3} "
                        f"content-search={'HIT ' if found else 'MISS'}")
        rows.append(f"    => stored content survived: {survived}")
    return "\n".join(rows)


def main() -> None:
    jobs = [("A", i) for i in range(N)]
    with ThreadPoolExecutor(max_workers=N) as pool:
        for text in pool.map(lambda a: run(*a), jobs):
            print(text)
    with ThreadPoolExecutor(max_workers=N) as pool:
        for text in pool.map(lambda a: run(*a), [("B", i) for i in range(N)]):
            print(text)
    print("--- done ---")


if __name__ == "__main__":
    main()
