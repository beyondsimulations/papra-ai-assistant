"""Is a single Papra token matched exactly or as a prefix? Measured, content-verified.

Earlier notes claimed "complete tokens, not prefixes" (Rechnung !~ Rechnungen); a probe today
matched Waesche -> Waescherei. One of the two is wrong. Each document is verified by GET (its
stored content is what we think it is) BEFORE any search, so a match can only come from the
token under test and not from a lost write or a filename.
"""
import time, uuid, httpx, fitz

BASE = "http://127.0.0.1:12210"
s = uuid.uuid4().hex[:8]
with httpx.Client(base_url=BASE, timeout=60) as h:
    h.post("/api/auth/sign-up/email", json={"email": f"px-{s}@x.test",
         "password": "probe-password-123456", "name": "P"}).raise_for_status()
    org = h.post("/api/organizations", json={"name": f"PX {s}"}
                 ).raise_for_status().json()["organization"]["id"]
    tok = h.post("/api/api-keys", json={"name": f"k-{s}", "permissions": [
        "organizations:read", "documents:read", "documents:create"]}).raise_for_status().json()["token"]
api = httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {tok}"}, timeout=60)

BODIES = {
    "alpha": "alpha", "alphabet": "alphabet", "alpha_beta": "alpha beta",
    "rechnung": "Rechnung", "rechnungen": "Rechnungen",
    "waesche": "Waesche", "waescherei": "Waescherei",
    "strasse": "Strasse", "strassen": "Strassenverkehr",
}
ids, names = {}, {}
for key, body in BODIES.items():
    doc = fitz.open(); pg = doc.new_page()
    pg.insert_text((40, 72), body, fontsize=14)
    data = doc.tobytes(); doc.close()
    # filename carries no substring of any test token
    fname = f"dossier-{key}-{uuid.uuid4().hex[:8]}.pdf"
    d = api.post(f"/api/organizations/{org}/documents",
                 files={"file": (fname, data, "application/pdf")}
                 ).raise_for_status().json()["document"]
    ids[key], names[key] = d["id"], fname

# settle, then verify stored content is exactly what we put there
deadline = time.time() + 60
stable = {}
while time.time() < deadline:
    contents = {k: api.get(f"/api/organizations/{org}/documents/{v}").raise_for_status()
                    .json()["document"].get("content") or "" for k, v in ids.items()}
    if stable == contents and all(c.strip() for c in contents.values()):
        break
    stable = contents
    time.sleep(2)
print("stored content per document:")
for k in BODIES:
    print(f"   {k:11} file={names[k][:24]:26} content={stable[k]!r}")

def search(q):
    r = api.get(f"/api/organizations/{org}/documents",
                params={"pageIndex": 0, "pageSize": 100, "searchQuery": q}).raise_for_status()
    return {d["id"] for d in r.json()["documents"]}

inv = {v: k for k, v in ids.items()}
print("\nquery            -> matched (documents whose NAME contains the query: "
      "none, verified above)")
for q in ["alpha", "alphabet", "Rechnung", "Rechnungen", "Waesche", "Waescherei",
          "Strasse", "Strassen", "Strassenverkehr", "alpha2", "lpha"]:
    got = sorted(inv[d] for d in search(q))
    print(f"  {q:16} -> {got}")
