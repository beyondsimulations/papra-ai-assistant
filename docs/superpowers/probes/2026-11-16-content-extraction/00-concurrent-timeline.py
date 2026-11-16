"""Probe #6: is Papra's FTS content sync RACY rather than reliable?

One uncontrolled observation (spike #5) lost content search for a document that
was matched seconds earlier, with no write in between (the only intervening call
was a 401'd DELETE). This is the exact pattern the 2026-11-16 matrix could not
reproduce. Instrumented properly:

  * 6 documents, distinct fresh phrases, staggered starts
  * each polled on its own timeline: t+0, 3, 10, 30, 60, 120, 180 s
  * per tick: content search, name search (distinguishes 'index row gone' from
    'content column dropped out of the index'), GET content length
  * the first document that loses search gets ONE repair PATCH(name+content)
    to test repair, exactly as probes #4 -> #5 did

Concurrent so wall time stays ~200 s. Reports a per-document timeline.
"""

import concurrent.futures as cf
import threading
import time
import uuid

import httpx

BASE = "http://127.0.0.1:12210"
PDF = (b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
       b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
       b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 200 200]>>endobj\n"
       b"trailer<</Root 1 0 R>>\n%%EOF\n")
TICKS = [0, 3, 10, 30, 60, 120, 180]
DOCS = 6

_lock = threading.Lock()
_org: tuple[str, str] | None = None


def org_and_key() -> tuple[str, str]:
    global _org
    with _lock:
        if _org is None:
            s = uuid.uuid4().hex[:8]
            with httpx.Client(base_url=BASE, timeout=30) as h:
                h.post("/api/auth/sign-up/email", json={
                    "email": f"p6-{s}@x.test", "password": "probe-password-123456",
                    "name": "P"}).raise_for_status()
                orgid = h.post("/api/organizations", json={"name": f"Race {s}"}
                               ).raise_for_status().json()["organization"]["id"]
                tok = h.post("/api/api-keys", json={
                    "name": f"k-{s}",
                    "permissions": ["organizations:read", "documents:read",
                                    "documents:create", "documents:update"],
                }).raise_for_status().json()["token"]
            _org = (orgid, tok)
    return _org


def trial(index: int) -> list[str]:
    orgid, tok = org_and_key()
    phrase = f"zqxt{index}-{uuid.uuid4().hex[:6]}"      # fresh per trial, single token
    name_token = f"wnvk{index}{uuid.uuid4().hex[:6]}"    # unrelated to the phrase
    name = f"{name_token}.pdf"
    log: list[str] = []
    with httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {tok}"},
                      timeout=30) as api:
        doc = api.post(f"/api/organizations/{orgid}/documents",
                       files={"file": (name, PDF, "application/pdf")}
                       ).raise_for_status().json()["document"]
        did = doc["id"]
        t0 = time.monotonic()
        api.patch(f"/api/organizations/{orgid}/documents/{did}",
                  json={"content": f"alpha {phrase} omega"}).raise_for_status()
        wrote = time.monotonic() - t0
        repaired = False
        for tick in TICKS:
            wait = t0 + tick - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            hits = api.get(f"/api/organizations/{orgid}/documents",
                           params={"pageIndex": 0, "pageSize": 10,
                                   "searchQuery": phrase}).json()["documents"]
            named = api.get(f"/api/organizations/{orgid}/documents",
                            params={"pageIndex": 0, "pageSize": 10,
                                    "searchQuery": name_token}).json()["documents"]
            listed = api.get(f"/api/organizations/{orgid}/documents",
                             params={"pageIndex": 0, "pageSize": 50}).json()["documents"]
            in_list = any(d["id"] == did for d in listed)
            content_len = len(api.get(
                f"/api/organizations/{orgid}/documents/{did}"
            ).json()["document"].get("content") or "")
            found = any(d["id"] == did for d in hits)
            log.append(
                f"doc{index} t+{tick:<3} content-search={'HIT ' if found else 'MISS'}"
                f" name-search={'hit ' if any(d['id'] == did for d in named) else 'miss'}"
                f" listed={'yes' if in_list else 'no'} stored-content={content_len}"
                f" total-listed={len(listed)}")
            if not found and not repaired and index < 3:
                repaired = True
                api.patch(f"/api/organizations/{orgid}/documents/{did}",
                          json={"name": name, "content": f"alpha {phrase} omega"})
                log.append(f"doc{index} t+{tick} REPAIR PATCH(name+content) issued")
        log.append(f"doc{index} (patch latency {wrote*1000:.0f} ms)")
    return log


def main() -> None:
    with cf.ThreadPoolExecutor(max_workers=DOCS) as pool:
        for log in pool.map(trial, range(DOCS)):
            print("\n".join(log))
    print("--- done ---")


if __name__ == "__main__":
    main()
