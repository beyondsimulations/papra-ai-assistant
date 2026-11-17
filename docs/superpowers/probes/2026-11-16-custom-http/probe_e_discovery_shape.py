"""Probe E: how do we detect 'this document has no content' without fetching files?

The recovery design needs a predicate for "content missing" that runs over a whole
organization on a schedule. If the answer is 'download every document', the sweep is a
bandwidth event and gets run rarely, which defeats the purpose; if the listing or the
document detail already carries content presence, it is cheap. This reads the payloads
and reports the field names, which is the only way to know — the API surface was guessed
wrongly often enough in this project to check rather than recall.

Run against the scratch stack, which already holds documents with and without content:
  .venv/bin/python docs/superpowers/probes/2026-11-16-custom-http/probe_e_discovery_shape.py
"""

import json
import time
import uuid

import fitz
import httpx

BASE = "http://127.0.0.1:12210"


def provision(client: httpx.Client) -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:8]
    client.post("/api/auth/sign-up/email", json={
        "email": f"probee-{suffix}@papra-suite.test", "password": "probe-password-123",
        "name": "Probe E"}, timeout=30).raise_for_status()
    org = client.post("/api/organizations", json={"name": f"Probe E {suffix}"}, timeout=30)
    org.raise_for_status()
    key = client.post("/api/api-keys", json={
        "name": f"probee-{suffix}",
        "permissions": ["organizations:read", "documents:create", "documents:read",
                        "documents:update"]}, timeout=30)
    key.raise_for_status()
    return org.json()["organization"]["id"], key.json()["token"]


def fixture_pdf(text: str) -> bytes:
    doc = fitz.open()
    page = doc.new_page()
    if text:
        page.insert_text((72, 96), text, fontsize=14)
    data = doc.tobytes()
    doc.close()
    return data


def main() -> None:
    with httpx.Client(base_url=BASE) as admin:
        org_id, token = provision(admin)
    api = httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {token}"})

    # Three states a content audit has to distinguish: text extracted, nothing extracted,
    # and text written by us. A hand-written PDF skeleton would not do: it is not a document
    # Papra's extractor can look at, so the "no content" case would have been an artifact of
    # my fixture rather than a property of the API.
    for name, blob in (("has-text.pdf", fixture_pdf("Rechnung QRZ7DISCOVERY Betrag 12,34")),
                       ("blank.pdf", fixture_pdf("")),
                       ("will-be-emptied.pdf", fixture_pdf("Vorläufiger Text QRZ7EMPTIED"))):
        response = api.post(f"/api/organizations/{org_id}/documents",
                            files={"file": (name, blob, "application/pdf")}, timeout=60)
        if response.status_code >= 400:
            print(f"{name}: upload {response.status_code} {response.text[:120]}")
            continue

    # Give extraction a chance to land, then add the third state by emptying a document
    # we already wrote: that is the state a content audit must detect, and it is worth
    # knowing whether the API will even accept an empty content write.
    time.sleep(8.0)
    all_docs = api.get(f"/api/organizations/{org_id}/documents",
                       params={"pageIndex": 0, "pageSize": 25}, timeout=30).json().get(
                           "documents", [])
    target = next((d for d in all_docs if d.get("name") == "will-be-emptied.pdf"), None)
    if target is not None:
        emptied = api.patch(f"/api/organizations/{org_id}/documents/{target['id']}",
                            json={"content": ""}, timeout=30)
        print(f"PATCH(content='') -> {emptied.status_code} {emptied.text[:140]}")
        read_back = api.get(f"/api/organizations/{org_id}/documents/{target['id']}",
                            timeout=30).json()["document"].get("content")
        print(f"  read back: {read_back!r} (type {type(read_back).__name__})")

    listing = api.get(f"/api/organizations/{org_id}/documents",
                      params={"pageIndex": 0, "pageSize": 10}, timeout=30)
    listing.raise_for_status()
    payload = listing.json()
    print(f"listing top-level keys : {sorted(payload)}")
    documents = payload.get("documents", [])
    if documents:
        print(f"listing document keys  : {sorted(documents[0])}")
        for document in documents[:3]:
            printable = {k: v for k, v in document.items()
                         if isinstance(v, (str, int, float, bool, type(None)))}
            trimmed = {k: (str(v)[:48]) for k, v in printable.items()}
            print(f"  {json.dumps(trimmed, indent=None)[:520]}")
        content_keys = [k for k in documents[0] if "content" in k.lower()
                        or "text" in k.lower() or "ocr" in k.lower()
                        or "extract" in k.lower()]
        print(f"content-ish listing fields: {content_keys or 'NONE'}")

    if documents:
        document_id = documents[0]["id"]
        detail = api.get(f"/api/organizations/{org_id}/documents/{document_id}", timeout=30)
        detail.raise_for_status()
        detail_document = detail.json()["document"]
        print(f"\ndetail document keys : {sorted(detail_document)}")
        detail_content_keys = [k for k in detail_document if "content" in k.lower()
                               or "text" in k.lower() or "ocr" in k.lower()
                               or "extract" in k.lower()]
        print(f"content-ish detail fields: {detail_content_keys or 'NONE'}")
        # Size of the payload a content audit would pay for, per document.
        print(f"detail payload size  : {len(detail.content)} bytes for one document")
        print(f"listing payload size : {len(listing.content)} bytes for "
              f"{len(documents)} documents")
        print(f"does detail carry the text inline? "
              f"{'content' in detail_document} "
              f"(len={len(detail_document.get('content') or '')})")
    api.close()


if __name__ == "__main__":
    main()
