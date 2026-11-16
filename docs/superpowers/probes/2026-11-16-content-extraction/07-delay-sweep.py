"""Probe #11: where is the boundary, and does the wipe happen exactly once?

Arms: patch content at upload+{1,3,5,8,15} s, n=3 each, token in content only.
Also, for the documents whose content vanished, one LATE re-write at t+30 to test
whether the async writer runs exactly once (repair works) or periodically.
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
LOCKS: list[float] = [1, 3, 5, 8, 15]
N = 3
_state: dict[str, str] = {}
_lock = threading.Lock()


def org_and_key() -> tuple[str, str]:
    with _lock:
        if not _state:
            s = uuid.uuid4().hex[:8]
            with httpx.Client(base_url=BASE, timeout=30) as h:
                h.post("/api/auth/sign-up/email", json={
                    "email": f"p11-{s}@x.test", "password": "probe-password-123456",
                    "name": "P"}).raise_for_status()
                _state["org"] = h.post("/api/organizations", json={"name": f"P11 {s}"}
                                       ).raise_for_status().json()["organization"]["id"]
                _state["tok"] = h.post("/api/api-keys", json={
                    "name": f"k-{s}",
                    "permissions": ["organizations:read", "documents:read",
                                    "documents:create", "documents:update"],
                }).raise_for_status().json()["token"]
    return _state["org"], _state["tok"]


def run(delay: float, index: int) -> str:
    org, tok = org_and_key()
    token = f"bnd{int(delay)}x{index}{uuid.uuid4().hex[:5]}"
    with httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {tok}"},
                      timeout=30) as api:
        body = PDF + f"\n% {uuid.uuid4().hex}\n".encode()
        doc = api.post(f"/api/organizations/{org}/documents",
                       files={"file": (f"b-{delay}-{index}.pdf", body, "application/pdf")}
                       ).raise_for_status().json()["document"]
        did = doc["id"]
        uploaded = time.monotonic()
        remaining = delay - (time.monotonic() - uploaded)
        if remaining > 0:
            time.sleep(remaining)
        api.patch(f"/api/organizations/{org}/documents/{did}",
                  json={"content": f"alpha {token} omega"}).raise_for_status()
        patched = time.monotonic()
        seen_empty = False
        for tick in (1, 3, 8, 20, 40):
            target = patched + tick
            while time.monotonic() < target:
                time.sleep(0.05)
            d = api.get(f"/api/organizations/{org}/documents/{did}").json()["document"]
            if not (d.get("content") or ""):
                seen_empty = True
        if seen_empty:
            api.patch(f"/api/organizations/{org}/documents/{did}",
                      json={"content": f"alpha {token} omega"}).raise_for_status()
            time.sleep(5)
            d = api.get(f"/api/organizations/{org}/documents/{did}").json()["document"]
            repair = len(d.get("content") or "")
            return (f"delay {delay:>4} s doc{index}: WIPED, repair at ~t+40 -> "
                    f"content_len after repair = {repair}")
        return f"delay {delay:>4} s doc{index}: survived to t+40"


def main() -> None:
    jobs = [(d, i) for d in LOCKS for i in range(N)]
    with ThreadPoolExecutor(max_workers=6) as pool:
        for text in pool.map(lambda a: run(*a), jobs):
            print(text)
    print("--- done ---")


if __name__ == "__main__":
    main()
