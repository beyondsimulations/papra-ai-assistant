"""Probe #9: does a PATCH(content) write survive? Poll GET(content) on a fine clock.

Motivation: probe #8 found 6/6 documents with `content: ""` two seconds after a
200 PATCH(content), while an identical-shaped PATCH in an earlier probe was
searchable. Either the write never lands or something asynchronous clears it.
Four arms, same fixture, n=4 each, paired.

  A  upload -> PATCH(content)                      -> poll content
  B  upload -> PATCH(name + content)               -> poll content
  C  upload -> sleep 5 -> PATCH(content)           -> poll content
  D  upload only (control: does Papra write content by itself?) -> poll content
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
ORG = {}
_LOCK = threading.Lock()


def org_and_key() -> tuple[str, str]:
    with _LOCK:
      if not ORG:
        s = uuid.uuid4().hex[:8]
        with httpx.Client(base_url=BASE, timeout=30) as h:
            h.post("/api/auth/sign-up/email", json={
                "email": f"p9-{s}@x.test", "password": "probe-password-123456",
                "name": "P"}).raise_for_status()
            ORG["org"] = h.post("/api/organizations", json={"name": f"P9 {s}"}
                                ).raise_for_status().json()["organization"]["id"]
            ORG["tok"] = h.post("/api/api-keys", json={
                "name": f"k-{s}",
                "permissions": ["organizations:read", "documents:read",
                                "documents:create", "documents:update"],
            }).raise_for_status().json()["token"]
    return ORG["org"], ORG["tok"]


def poll(api: httpx.Client, org: str, did: str, label: str, out: list[str]) -> None:
    for tick in (0.0, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0):
        waited = api.get(f"/api/organizations/{org}/documents/{did}")
        doc = waited.json()["document"]
        hits = api.get(f"/api/organizations/{org}/documents",
                       params={"pageIndex": 0, "pageSize": 10,
                               "searchQuery": label}).json()["documents"]
        out.append(f"    t+{tick:<4} content={doc.get('content')!r:30} "
                   f"name={doc.get('name')!r:20} search={'HIT' if any(h['id'] == did for h in hits) else 'MISS'}")
        time.sleep(max(0.0, tick - (0 if tick == 0 else 0)) if tick else 0)


def arm(name: str, index: int) -> str:
    org, tok = org_and_key()
    label = f"wmzk{index}{uuid.uuid4().hex[:5]}"
    out: list[str] = [f"{name} doc{index} token={label}"]
    with httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {tok}"},
                      timeout=30) as api:
        body = PDF + f"\n% {uuid.uuid4().hex}\n".encode()
        doc = api.post(f"/api/organizations/{org}/documents",
                       files={"file": (f"{label}.pdf", body, "application/pdf")}
                       ).raise_for_status().json()["document"]
        did = doc["id"]
        t0 = time.monotonic()

        def stamp() -> float:
            return round(time.monotonic() - t0, 2)

        if name == "A":
            r = api.patch(f"/api/organizations/{org}/documents/{did}",
                          json={"content": f"alpha {label} omega"})
            out.append(f"    PATCH(content) at {stamp()}s -> {r.status_code}")
        elif name == "B":
            r = api.patch(f"/api/organizations/{org}/documents/{did}",
                          json={"name": f"renamed-{label}.pdf",
                                "content": f"alpha {label} omega"})
            out.append(f"    PATCH(name,content) at {stamp()}s -> {r.status_code}")
        elif name == "C":
            time.sleep(5)
            r = api.patch(f"/api/organizations/{org}/documents/{did}",
                          json={"content": f"alpha {label} omega"})
            out.append(f"    PATCH(content) at {stamp()}s -> {r.status_code}")
        else:
            out.append(f"    no write (control) at {stamp()}s")

        for tick in (0.0, 0.3, 0.7, 1.5, 3.0, 6.0, 12.0, 25.0):
            target = t0 + tick
            while time.monotonic() < target:
                time.sleep(0.05)
            d = api.get(f"/api/organizations/{org}/documents/{did}"
                        ).json()["document"]
            hits = api.get(f"/api/organizations/{org}/documents",
                           params={"pageIndex": 0, "pageSize": 20,
                                   "searchQuery": label}).json()["documents"]
            out.append(f"    t+{tick:<5} content={d.get('content')!r:32} "
                       f"name={d.get('name')!r:26} "
                       f"search={'HIT ' if any(h['id'] == did for h in hits) else 'MISS'}")
    return "\n".join(out)


def main() -> None:
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(arm, name, i)
                   for i, name in enumerate(["A", "B", "C", "D"] * 1)]
        for f in futures:
            print(f.result())
    print("--- done ---")


if __name__ == "__main__":
    main()
