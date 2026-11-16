"""Umlauts, eszett and accented characters: how does Papra tokenise them?

Decides the tokeniser. If 'Wäsche' is indexed as 'w'+'sche' (because the umlaut is treated as a
separator), then a search for 'Wäsche' is a search for the prefix 'w' — noise. If it is kept,
the tokeniser must NOT strip diacritics.
"""
import time, uuid, httpx, fitz

BASE = "http://127.0.0.1:12210"
s = uuid.uuid4().hex[:8]
with httpx.Client(base_url=BASE, timeout=60) as h:
    h.post("/api/auth/sign-up/email", json={"email": f"u-{s}@x.test",
         "password": "probe-password-123456", "name": "U"}).raise_for_status()
    org = h.post("/api/organizations", json={"name": f"U {s}"}
                 ).raise_for_status().json()["organization"]["id"]
    tok = h.post("/api/api-keys", json={"name": f"k-{s}", "permissions": [
        "organizations:read", "documents:read", "documents:create"]}).raise_for_status().json()["token"]
api = httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {tok}"}, timeout=60)

BODIES = {
    "wasche_umlaut": "Wäsche",
    "strasse_umlaut": "Straße",
    "beta_mwst": "MwSt 19 Prozent",
    "gamma_nr": "Rechnung Nr. 4711/2026",
    "delta_gruen": "Grüße aus Köln",
    "epsilon_cafe": "Café à Paris",
    "zeta_at": "info@beispiel.de",
}
ids = {}
for key, body in BODIES.items():
    doc = fitz.open(); pg = doc.new_page()
    pg.insert_text((40, 72), body, fontsize=13)
    data = doc.tobytes(); doc.close()
    d = api.post(f"/api/organizations/{org}/documents",
                 files={"file": (f"beleg-{uuid.uuid4().hex[:8]}.pdf", data, "application/pdf")}
                 ).raise_for_status().json()["document"]
    ids[key] = d["id"]

deadline, stable = time.time() + 90, {}
while time.time() < deadline:
    c = {k: api.get(f"/api/organizations/{org}/documents/{v}").raise_for_status()
            .json()["document"].get("content") or "" for k, v in ids.items()}
    if stable == c and all(x.strip() for x in c.values()):
        break
    stable = c; time.sleep(2)
print("stored content (what fitz produced and Papra kept):")
for k, v in BODIES.items():
    print(f"   {k:15} wrote={v!r:26} stored={stable[k]!r}")

def search(q):
    r = api.get(f"/api/organizations/{org}/documents",
                params={"pageIndex": 0, "pageSize": 100, "searchQuery": q})
    if r.status_code != 200:
        return {f"HTTP-{r.status_code}-{r.json()}"}
    return {d["id"] for d in r.json()["documents"]}
inv = {v: k for k, v in ids.items()}
def show(q):
    got = search(q)
    got = sorted(inv.get(d, d) for d in got)
    print(f"  q={q!r:22} -> {got}")

print("\nqueries (matched by CONTENT only; every filename is 'beleg-<rand>.pdf'):")
for q in ["Wäsche", "wäsche", "Waesche", "Wsch", "Straße", "Strasse", "Grüße", "Gruss",
          "Köln", "Koln", "MwSt", "Mw", "Nr", "4711", "4711/2026", "Café", "Cafe", "info",
          "beispiel.de", "beispiel", "19"]:
    show(q)
