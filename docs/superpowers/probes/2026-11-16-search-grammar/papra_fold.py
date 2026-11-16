"""One word per document, no other text. Does Papra fold diacritics when matching?"""
import time, uuid, httpx, fitz
BASE = "http://127.0.0.1:12210"
s = uuid.uuid4().hex[:8]
with httpx.Client(base_url=BASE, timeout=60) as h:
    h.post("/api/auth/sign-up/email", json={"email": f"f-{s}@x.test",
         "password": "probe-password-123456", "name": "F"}).raise_for_status()
    org = h.post("/api/organizations", json={"name": f"F {s}"}
                 ).raise_for_status().json()["organization"]["id"]
    tok = h.post("/api/api-keys", json={"name": f"k-{s}", "permissions": [
        "organizations:read", "documents:read", "documents:create"]}).raise_for_status().json()["token"]
api = httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {tok}"}, timeout=60)

WORDS = {"wa": "Wäsche", "oe": "Köln", "ue": "Grüße", "sz": "Straße",
         "eacc": "Café", "aacc": "Màloni", "plain": "Tischen"}
ids = {}
for k, w in WORDS.items():
    doc = fitz.open(); doc.new_page().insert_text((40, 72), w, fontsize=16)
    data = doc.tobytes()
    d = api.post(f"/api/organizations/{org}/documents",
                 files={"file": (f"beleg-{uuid.uuid4().hex[:8]}.pdf", data, "application/pdf")}
                 ).raise_for_status().json()["document"]
    ids[k] = d["id"]; doc.close()

deadline, stable = time.time() + 90, {}
while time.time() < deadline:
    c = {k: api.get(f"/api/organizations/{org}/documents/{v}").raise_for_status()
            .json()["document"].get("content") or "" for k, v in ids.items()}
    if stable == c and all(x.strip() for x in c.values()):
        break
    stable = c; time.sleep(2)
print({k: stable[k] for k in WORDS})

def search(q):
    r = api.get(f"/api/organizations/{org}/documents",
                params={"pageIndex": 0, "pageSize": 100, "searchQuery": q}).raise_for_status()
    return sorted(k for k, v in ids.items() if v in {d["id"] for d in r.json()["documents"]})

print("\nword in doc  | exact query      -> match | folded    | shorter prefix | wrong fold")
tests = [("wa", "Wäsche", "Waesche", "Wä", "W"),
         ("oe", "Köln", "Koln", "Köl", "Koln"),
         ("ue", "Grüße", "Grusse", "Grü", "Gruss"),
         ("sz", "Straße", "Strasse", "Stra", "Stras"),
         ("eacc", "Café", "Cafe", "Caf", "Caff"),
         ("aacc", "Màloni", "Maloni", "Màl", "Maloni")]
for expect, exact, folded, shorter, wrong in tests:
    print(f"  {WORDS[expect]:10} | {exact:14} -> {search(exact)} | {folded:9} -> {search(folded)} "
          f"| {shorter:6} -> {search(shorter)} | {wrong:8} -> {search(wrong)}")
