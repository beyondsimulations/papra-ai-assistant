"""Probe A2: is Papra's default extraction good enough for *this* corpus?

Probe A showed the default `internal` strategy OCRs an image-only scan and gets a
200-dpi synthetic page exactly right in ~3 s. That is the easiest input an OCR will
ever see, so it proves nothing about the documents this pipeline actually ingests.

Two harder questions, both decision-changing:

1. **Diacritics.** The image bundles `eng.traineddata` and nothing else was found in
   it. If English language data reads `Wäsche` as `Wasche`, the damage is not
   cosmetic: Papra's search does not fold diacritics (measured — `Wäsche` is found by
   `Wäsche` and `Wä`, never by `Waesche`), so a document whose umlauts were stripped
   at extraction time becomes unfindable for anyone who types the word as printed.
2. **Degradation.** Rotation, low contrast, blur and JPEG artifacts are what a
   forwarded photograph of an invoice looks like, as opposed to a rendered page.

Run:  .venv/bin/python docs/superpowers/probes/2026-11-16-custom-http/probe_a2_harder_inputs.py
"""

import io
import time
import uuid

import httpx
import fitz

BASE = "http://127.0.0.1:12210"

UMlaut_TOKEN = "QRZ7UMWAESCHE"
DEGRADED_TOKEN = "QRZ7DEGRADED"

# No ASCII renderings of the tested words anywhere on the page: with a control line saying
# "Strasse", a hit on "Strasse" would stop meaning the extraction preserved "Straße".
GERMAN_LINES = [
    "Rechnung {token}",
    "Die Wäsche Straße Grüße Café Köln",
    "Betrag: 1.234,56 EUR",
]


def render(text: str, *, dpi: int = 200) -> "fitz.Pixmap":
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 96), text, fontsize=13)
    pixmap = page.get_pixmap(dpi=dpi)
    doc.close()
    return pixmap


def image_only_pdf(pixmap: "fitz.Pixmap") -> bytes:
    """A page whose only content is one bitmap: no text layer, by assertion."""
    doc = fitz.open()
    page = doc.new_page(width=612, height=612 * pixmap.height / pixmap.width)
    page.insert_image(page.rect, pixmap=pixmap)
    assert page.get_text().strip() == "", "not image-only: a text layer exists"
    data = doc.tobytes()
    doc.close()
    return data


def degraded_pixmap(text: str) -> "fitz.Pixmap":
    """Low JPEG quality plus a halved resolution: what a forwarded photo loses.

    Approximation, stated plainly: no rotation, no blur, no perspective skew, because
    PyMuPDF cannot degrade an image the way an image library would and this probe adds
    no new dependency to do it. So read the result as "a bad scan", not "a phone
    photo", and do not conclude from a pass here that real photographs are fine.
    """
    pixmap = render(text, dpi=300)
    restored = fitz.Pixmap(pixmap.tobytes("jpg", jpg_quality=45))
    restored.shrink(1)          # half resolution: the detail a degraded scan loses
    return restored


def sign_up(client: httpx.Client) -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:8]
    client.post("/api/auth/sign-up/email", json={
        "email": f"probea2-{suffix}@papra-suite.test", "password": "probe-password-123",
        "name": "Probe A2"}, timeout=30).raise_for_status()
    org = client.post("/api/organizations", json={"name": f"Probe A2 {suffix}"}, timeout=30)
    org.raise_for_status()
    key = client.post("/api/api-keys", json={
        "name": f"probea2-{suffix}",
        "permissions": ["organizations:read", "documents:create", "documents:read",
                        "documents:update"]}, timeout=30)
    key.raise_for_status()
    return org.json()["organization"]["id"], key.json()["token"]


def settle(client: httpx.Client, org_id: str, document_id: str, *, window_s: float = 30.0):
    deadline = time.monotonic() + window_s
    previous = ""
    stable_at = None
    started = time.monotonic()
    while time.monotonic() < deadline:
        doc = client.get(f"/api/organizations/{org_id}/documents/{document_id}",
                         timeout=30).json()["document"]
        current = doc.get("content") or ""
        if current and current == previous:
            if stable_at is None:
                stable_at = time.monotonic()
            if time.monotonic() - stable_at >= 2.0:
                return time.monotonic() - started, current
        else:
            stable_at = None
        previous = current
        time.sleep(0.5)
    return float("nan"), previous


def hits(client: httpx.Client, org_id: str, document_id: str, query: str) -> str:
    """'hit'/'miss' for one named document, in an org holding only that document.

    The first run counted org-wide results while two fixtures shared one org, and the page
    carried ASCII control words for the very words under test. Both confounds are removed
    here: one org per fixture, and the assertion is on this document's id.
    """
    response = client.get(f"/api/organizations/{org_id}/documents",
                          params={"pageIndex": 0, "pageSize": 10, "searchQuery": query},
                          timeout=30)
    response.raise_for_status()
    payload = response.json()
    assert payload.get("documentsCount", 0) <= 1, "org is not single-document"
    ids = [document.get("id") for document in payload.get("documents", [])]
    return "hit" if document_id in ids else "miss"


def main() -> None:
    umlaut_text = "\n".join(GERMAN_LINES).format(token=UMlaut_TOKEN)
    degraded_text = (f"Rechnung {DEGRADED_TOKEN}\n"
                     "Die Wäsche geht durch die Straße.\nBetrag 99,95 EUR")

    fixtures = [
        ("umlauts-200dpi.pdf", image_only_pdf(render(umlaut_text, dpi=200)),
         UMlaut_TOKEN, umlaut_text),
        ("degraded-150dpi.jpg.pdf", image_only_pdf(degraded_pixmap(degraded_text)),
         DEGRADED_TOKEN, degraded_text),
    ]

    for name, data, token_text, source in fixtures:
        with httpx.Client(base_url=BASE) as admin:
            org_id, token = sign_up(admin)
        with httpx.Client(base_url=BASE,
                          headers={"Authorization": f"Bearer {token}"}) as api:
            response = api.post(f"/api/organizations/{org_id}/documents",
                                files={"file": (name, data, "application/pdf")}, timeout=60)
            response.raise_for_status()
            document_id = response.json()["document"]["id"]
            elapsed, content = settle(api, org_id, document_id)
            print(f"\n=== {name} ({len(data)} bytes) — stable by {elapsed:.1f}s "
                  f"(includes a 2 s stability confirmation, so this is an upper bound "
                  f"on detection, not an OCR runtime) ===")
            print(f"  extracted: {content!r}")
            print(f"  own token found in text   : {token_text in content}")
            print(f"  own token search hit      : "
                  f"{hits(api, org_id, document_id, token_text)}")
            # Each word is asked in both the printed form and the ASCII form the
            # extractor might have substituted. A hit on the ASCII form and a miss on
            # the printed one is precisely the failure that makes a document
            # unfindable: Papra does not fold diacritics at query time either.
            for word in ("Wäsche", "Waesche", "Wasche", "Straße", "Strasse",
                         "Grüße", "Grusse", "Café", "Cafe", "Köln", "Koln"):
                print(f"  search {word!r:12} -> "
                      f"{hits(api, org_id, document_id, word)}")


if __name__ == "__main__":
    main()
