"""Papra query grammar, measured. What may the MCP tokeniser forward?

Questions the tokeniser spec depends on:
  - umlauts: is 'Wäsche' one token? is it found by 'wäsche' / 'wasche' / 'sche'?
  - case: is a capitalised token found by a lowercase query?
  - short tokens: is a 2-char token indexed at all?
  - punctuation: is 'AZ-99QK' one token or two upstream?
  - operators: do AND/OR/NOT and 'field:value' reach the matcher as syntax?
  - pagination: does searchQuery work with pageIndex/pageSize and report a count?
"""
import uuid, httpx, fitz

BASE = "http://127.0.0.1:12210"
s = uuid.uuid4().hex[:8]
with httpx.Client(base_url=BASE, timeout=60) as h:
    h.post("/api/auth/sign-up/email", json={"email": f"g-{s}@x.test",
         "password": "probe-password-123456", "name": "G"}).raise_for_status()
    org = h.post("/api/organizations", json={"name": f"G {s}"}
                 ).raise_for_status().json()["organization"]["id"]
    tok = h.post("/api/api-keys", json={"name": f"k-{s}", "permissions": [
        "organizations:read", "documents:read", "documents:create"]}).raise_for_status().json()["token"]

api = httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {tok}"}, timeout=60)


def make(body_text: str, label: str) -> str:
    doc = fitz.open()
    pg = doc.new_page()
    pg.insert_text((40, 72), body_text, fontsize=13)
    data = doc.tobytes()
    doc.close()
    d = api.post(f"/api/organizations/{org}/documents",
                 files={"file": (f"{label}-{uuid.uuid4().hex[:6]}.pdf", data, "application/pdf")}
                 ).raise_for_status().json()["document"]
    return d["id"]


def search(q: str) -> set[str]:
    r = api.get(f"/api/organizations/{org}/documents",
                params={"pageIndex": 0, "pageSize": 100, "searchQuery": q,
                        "sortField": "createdAt", "sortOrder": "desc"})
    if r.status_code != 200:
        return {f"HTTP{r.status_code}"}
    return {d["id"] for d in r.json()["documents"]}


umlaut = make("Waesche und Kaeselager", "umlaut")        # no umlauts in body
umlaut2 = make("Waescherei Kaesekessel", "umlaut2")      # different words, same stems
hyphen = make("AZ-99QK bezahlt", "hyphen")
short = make("az 4711", "short")
oper = make("alpha beta gamma", "oper")

import time
time.sleep(3)   # let extraction settle (measured: 0.24s for this fixture class)

cases = [
    ("umlaut body 'Waesche' -> q=Waesche", "Waesche", {umlaut}),
    ("umlaut body 'Waesche' -> q=waesche", "waesche", {umlaut}),
    ("body 'Waescherei' -> q=Waesche (prefix?)", "Waesche", {umlaut2}),
    ("body 'Kaeselager' -> q=Kaesel", "Kaesel", {umlaut}),
    ("body 'AZ-99QK' -> q=AZ-99QK", "AZ-99QK", {hyphen}),
    ("body 'AZ-99QK' -> q=az99qk", "az99qk", {hyphen}),
    ("body 'AZ-99QK' -> q=99qk", "99qk", {hyphen}),
    ("body 'AZ-99QK' -> q=AZ", "AZ", {hyphen}),
    ("body 'az 4711' -> q=az (2 chars)", "az", {short}),
    ("body 'az 4711' -> q=4711", "4711", {short}),
    ("body 'alpha beta gamma' -> q='alpha beta'", "alpha beta", {oper}),
    ("body only alpha -> q='alpha beta'", "alpha beta", set()),
    ("q='alpha AND beta' (operator?)", "alpha AND beta", set()),
    ("q='alpha OR beta'", "alpha OR beta", {oper}),
    ("q='name:alpha' (field syntax on body word)", "name:alpha", set()),
    ("q='content:alpha' (body word)", "content:alpha", {oper}),
    ("q='NOT beta'", "NOT beta", set()),
    ("q='alpha NOT beta'", "alpha NOT beta", set()),
    ("q='\"alpha beta\"' quoted phrase", '"alpha beta"', set()),
]
print("case                                                     | expected | matched")
for label, q, expect in cases:
    got = search(q)
    mark = "OK " if got == expect else "DIFF"
    print(f"{mark} {label:56} | {len(expect)} | {len(got)}")

print("\n--- pagination with searchQuery ---")
ids = {make(f"paginatordoku {uuid.uuid4().hex[:8]}", f"pag{i}") for i in range(5)}
time.sleep(3)
seen, page = set(), 0
while True:
    r = api.get(f"/api/organizations/{org}/documents",
                params={"pageIndex": page, "pageSize": 2, "searchQuery": "paginatordoku",
                        "sortField": "createdAt", "sortOrder": "desc"}).raise_for_status()
    data = r.json()
    docs = data["documents"]
    print(f"  page {page}: {len(docs)} docs, documentsCount={data.get('documentsCount')}")
    seen |= {d["id"] for d in docs}
    if not docs or page > 6:
        break
    page += 1
print("  paged union covers every created doc:", ids <= seen, f"({len(seen & ids)}/{len(ids)})")
