"""Probe #13: how long is Papra's extraction window for the fixture the pipeline
actually OCRs, and does a rename re-trigger it?

  A. upload the fitz two-page German PDF -> poll content until stable; latency
  B. after settle, PATCH(content) -> does it survive 60 s? (the pipeline's case)
  C. PATCH(name) alone -> does content change or vanish again? (rename re-trigger)
  D. concurrent burst: 10 uploads, then PATCH each at +0 s -> how many survive?
     (the queued-extraction case: does the window widen under load?)
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
        with httpx.Client(base_url=BASE, timeout=30) as h:
            h.post("/api/auth/sign-up/email", json={
                "email": f"p13-{s}@x.test", "password": "probe-password-123456",
                "name": "P"}).raise_for_status()
            _state["org"] = h.post("/api/organizations", json={"name": f"P13 {s}"}
                                   ).raise_for_status().json()["organization"]["id"]
            _state["tok"] = h.post("/api/api-keys", json={
                "name": f"k-{s}",
                "permissions": ["organizations:read", "documents:read",
                                "documents:create", "documents:update"],
            }).raise_for_status().json()["token"]
    return _state["org"], _state["tok"]


def make_german_pdf() -> bytes:
    pdf = fitz.open()
    page = pdf.new_page()
    page.insert_text((72, 72), "Rechnung Nr. 42")
    page.insert_text((72, 120), "Vielen Dank fuer Ihre Bestellung.")
    page = pdf.new_page()
    page.insert_text((72, 72), "Zahlungsbedingungen")
    page.insert_text((72, 120), "Zahlbar bis 15.11.2026.")
    data = pdf.tobytes()
    pdf.close()
    return data


def main() -> None:
    org, tok = org_and_key()
    api = httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {tok}"}, timeout=60)

    print("A. extraction latency for the fitz fixture")
    doc = api.post(f"/api/organizations/{org}/documents",
                   files={"file": ("rechnung.pdf", make_german_pdf(), "application/pdf")}
                   ).raise_for_status().json()["document"]
    did = doc["id"]
    t0 = time.monotonic()
    last = None
    stable_at = None
    while time.monotonic() - t0 < 60:
        c = api.get(f"/api/organizations/{org}/documents/{did}"
                    ).json()["document"].get("content") or ""
        if c and c == last:
            stable_at = time.monotonic() - t0
            break
        last = c
        time.sleep(0.1)
    print(f"   content non-empty+stable after {stable_at}s: {last!r}")

    print("\nB. PATCH(content) after settle -> survives 60 s?")
    marker = f"OCR-MARKER-{uuid.uuid4().hex[:8]}"
    api.patch(f"/api/organizations/{org}/documents/{did}",
              json={"content": f"# Rechnung 42\n\n{marker}"}).raise_for_status()
    for tick in (1, 5, 20, 60):
        time.sleep(tick if tick == 1 else 15)
        c = api.get(f"/api/organizations/{org}/documents/{did}"
                    ).json()["document"].get("content") or ""
        print(f"   +{tick:<3} marker present: {marker in c}")

    print("\nC. PATCH(name) alone -> content untouched?")
    api.patch(f"/api/organizations/{org}/documents/{did}",
              json={"name": "Rechnung 42.pdf"}).raise_for_status()
    for tick in (1, 10, 30):
        time.sleep(tick if tick == 1 else 10)
        d = api.get(f"/api/organizations/{org}/documents/{did}"
                    ).json()["document"]
        print(f"   +{tick:<3} name={d['name']!r} marker present: {marker in (d.get('content') or '')}")

    print("\nD. burst of 10 uploads, PATCH(content) at +0 s each")

    def one(i: int) -> str:
        token = f"burst{i}{uuid.uuid4().hex[:6]}"
        with httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {tok}"},
                          timeout=60) as a:
            d = a.post(f"/api/organizations/{org}/documents",
                       files={"file": (f"b{i}-{uuid.uuid4().hex[:6]}.pdf",
                                       make_german_pdf(), "application/pdf")}
                       ).raise_for_status().json()["document"]
            a.patch(f"/api/organizations/{org}/documents/{d['id']}",
                    json={"content": f"alpha {token} omega"}).raise_for_status()
            time.sleep(15)
            c = a.get(f"/api/organizations/{org}/documents/{d['id']}"
                      ).json()["document"].get("content") or ""
            hits = a.get(f"/api/organizations/{org}/documents",
                         params={"pageIndex": 0, "pageSize": 20,
                                 "searchQuery": token}).json()["documents"]
            found = any(h["id"] == d["id"] for h in hits)
            return (f"   doc{i}: marker survived={token in c} searchable={found} "
                    f"content={c[:48]!r}")

    with ThreadPoolExecutor(max_workers=10) as pool:
        for text in pool.map(one, range(10)):
            print(text)


if __name__ == "__main__":
    main()
