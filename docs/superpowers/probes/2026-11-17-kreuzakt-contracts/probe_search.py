"""Measure Kreuzakt's `/api/search` and `/api/documents` contracts against a container.

Task 3 measured upload + queue and said nothing about these two endpoints, and Task 5
has to encode their parameters. Nothing here is guessed: every fixture the Task 5
client tests are written from the live bodies captured below, by this file.

    docker compose -f docker-compose.test.yml -p papra-ai-assistant stop
    docker compose -f docker-compose.test.yml -p kreuzakt-bridge up -d --wait kreuzakt
    .venv/bin/python docs/superpowers/probes/2026-11-17-kreuzakt-contracts/probe_search.py
    docker compose -f docker-compose.test.yml -p kreuzakt-bridge down -v
    docker compose -f docker-compose.test.yml -p papra-ai-assistant start

Questions, in the order the client needs them:

  S0  does the sample even get extracted (health + queue) — the precondition
  S1  one `/api/search` body with hits, verbatim              -> fixture kreuzakt_search_page.json
  S2  one `/api/search` body with no hits, verbatim            -> fixture kreuzakt_search_empty.json
  S3  the upload response body, verbatim                       -> fixture kreuzakt_upload_response.json
  S4  does `limit` do anything on /api/search? is it clamped?
  S5  does `offset` do anything? does `page`? (same question, different name)
  S6  what fields does a hit carry — is there a score? is there content?
  D1  one `/api/documents` body, verbatim                      -> fixture kreuzakt_documents_page.json
  D2  do `limit`/`offset`/`page`/`since` do anything there?

Deliberately NOT reformatting-clean: this file is measurement evidence, and
`docs/superpowers/probes/` is excluded from ruff in pyproject.toml so that the
quoted log and the script that produced it cannot drift apart.
"""

import json
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent))
from make_pdf import build_pdf  # noqa: E402

BASE = "http://127.0.0.1:12400"
HERE = Path(__file__).parent
REPO = HERE.parents[3]
CAPTURE_PATH = HERE / "capture-search.json"
FIXTURES = REPO / "tests" / "fixtures"

capture = json.loads(CAPTURE_PATH.read_text()) if CAPTURE_PATH.exists() else {}
client = httpx.Client(base_url=BASE, timeout=120.0)

# Four documents: all four share the words "Rechnung" and "Miete" (make_pdf.py puts
# them in every sample), so a search for one of those must return a multi-hit set
# that a `limit`/`offset` could plausibly act on. Each also carries one unique ASCII
# token, so a single-hit response is reachable on demand.
MARKERS = ["Ziphoid", "Quorlund", "Vexbold", "Trumnik"]
NAMES = {
    "Ziphoid": "papra-search0001-11111111.pdf",
    "Quorlund": "papra-search0002-22222222.pdf",
    "Vexbold": "papra-search0003-33333333.pdf",
    "Trumnik": "papra-search0004-44444444.pdf",
}


def record(key, value):
    capture[key] = value
    CAPTURE_PATH.write_text(json.dumps(capture, indent=2, default=str))
    print(f"## {key}")
    print(json.dumps(value, indent=2, default=str)[:3000])


def write_fixture(name, body):
    FIXTURES.mkdir(parents=True, exist_ok=True)
    path = FIXTURES / name
    path.write_text(json.dumps(body, indent=2) + "\n")
    return {"fixture": str(path.relative_to(REPO)), "bytes": path.stat().st_size}


def body_of(response):
    try:
        return response.json()
    except Exception:  # noqa: BLE001 - a non-JSON body is itself the answer
        return {"__non_json__": response.text[:500]}


def search(**params):
    return client.get("/api/search", params=params)


def hit_ids(body):
    """Identify a result set by its ids, whatever key wraps the rows."""
    if not isinstance(body, dict):
        return None
    for key in ("results", "documents", "hits", "items", "entries"):
        rows = body.get(key)
        if isinstance(rows, list):
            return [row.get("id") for row in rows if isinstance(row, dict)]
    return None


def row_keys(body):
    ids = hit_ids(body)
    if not isinstance(body, dict) or ids is None:
        return None
    for key in ("results", "documents", "hits", "items", "entries"):
        rows = body.get(key)
        if isinstance(rows, list) and rows:
            return sorted(rows[0])
    return []


def wait_queue_settled(timeout_s=180.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        rows = client.get("/api/queue", params={"limit": 100}).json().get("entries", [])
        if rows and not any(r["status"] in ("pending", "processing") for r in rows):
            return rows
        time.sleep(1.0)
    raise SystemExit("queue did not settle")


# ---------------------------------------------------------------- S0 precondition
record("S0_health", client.get("/api/health").json())

upload_first = None
for marker in MARKERS:
    response = client.post(
        "/api/upload",
        files={"files": (NAMES[marker], build_pdf(marker), "application/pdf")},
    )
    if upload_first is None and response.status_code in (200, 201):
        upload_first = response.json()
    print("uploaded", marker, response.status_code)
record("S3_upload_response_verbatim", upload_first)
rows = wait_queue_settled()
record("S0_queue_after_uploads", {"rows": len(rows), "top_ids": [r["id"] for r in rows[:6]]})

# ---------------------------------------------------------------- S1/S2 the two bodies
with_hits = search(q=MARKERS[0])
no_hits = search(q="zzqxvbnmt-not-a-word-anywhere")
broad = search(q="Rechnung")
record("S1_search_one_hit", {"status": with_hits.status_code, "body": body_of(with_hits)})
record("S2_search_no_hits", {"status": no_hits.status_code, "body": body_of(no_hits)})
record("S4_search_broad_default", {"status": broad.status_code, "body": body_of(broad)})

# ---------------------------------------------------------------- S4/S5 the parameter matrix
base_ids = hit_ids(body_of(broad))
matrix = {"broad_default_ids": base_ids, "cases": []}
for label, params in [
    ("limit=1", {"q": "Rechnung", "limit": 1}),
    ("limit=2", {"q": "Rechnung", "limit": 2}),
    ("limit=1000", {"q": "Rechnung", "limit": 1000}),
    ("limit=0", {"q": "Rechnung", "limit": 0}),
    ("limit=negative", {"q": "Rechnung", "limit": -5}),
    ("limit=non-integer", {"q": "Rechnung", "limit": "abc"}),
    ("offset=1", {"q": "Rechnung", "offset": 1}),
    ("offset=2", {"q": "Rechnung", "offset": 2}),
    ("page=2", {"q": "Rechnung", "page": 2}),
    ("page=2&limit=1", {"q": "Rechnung", "page": 2, "limit": 1}),
    ("pageIndex=1&pageSize=1", {"q": "Rechnung", "pageIndex": 1, "pageSize": 1}),
    ("limit=2&offset=1", {"q": "Rechnung", "limit": 2, "offset": 1}),
    ("empty-result-with-limit", {"q": "zzqxvbnmt-not-a-word-anywhere", "limit": 2}),
]:
    response = search(**params)
    body = body_of(response)
    ids = hit_ids(body)
    matrix["cases"].append({
        "params": params,
        "status": response.status_code,
        "returned": len(ids) if ids is not None else None,
        "ids": ids,
        "same_ids_as_default_order": ids == base_ids if ids is not None else None,
        "total_keys": {k: v for k, v in body.items()
                       if not isinstance(v, list)} if isinstance(body, dict) else None,
        "body_head": json.dumps(body, default=str)[:300],
    })
record("S4_search_parameter_matrix", matrix)

# ---------------------------------------------------------------- S6 the hit's fields
broad_body = body_of(broad)
record("S6_search_hit_fields", {
    "envelope_keys": sorted(broad_body) if isinstance(broad_body, dict) else None,
    "hit_keys": row_keys(broad_body),
    "score_field_present": any(
        "score" in k.lower() for k in (row_keys(broad_body) or [])
    ),
    "content_field_present": any(
        "content" in k.lower() for k in (row_keys(broad_body) or [])
    ),
})

# ---------------------------------------------------------------- D1/D2 /api/documents
docs_default = client.get("/api/documents")
docs_limit = client.get("/api/documents", params={"limit": 1})
docs_offset = client.get("/api/documents", params={"offset": 1})
docs_page = client.get("/api/documents", params={"page": 2})
docs_since_future = client.get("/api/documents", params={"since": "2030-01-01T00:00:00Z"})
docs_since_past = client.get("/api/documents", params={"since": "2000-01-01T00:00:00Z"})

default_body = body_of(docs_default)


def doc_summary(response):
    body = body_of(response)
    ids = hit_ids(body)
    return {
        "status": response.status_code,
        "envelope_keys": sorted(body) if isinstance(body, dict) else None,
        "returned": len(ids) if ids is not None else None,
        "ids": ids,
        "row_keys": row_keys(body),
        "scalars": {k: v for k, v in body.items() if not isinstance(v, list)}
        if isinstance(body, dict) else None,
    }


record("D1_documents_default", {
    "query": "GET /api/documents (no params)",
    "body": default_body,
})
record("D2_documents_parameter_matrix", {
    "default_ids": hit_ids(default_body),
    "limit=1": doc_summary(docs_limit),
    "offset=1": doc_summary(docs_offset),
    "page=2": doc_summary(docs_page),
    "since=2030": doc_summary(docs_since_future),
    "since=2000": doc_summary(docs_since_past),
})

# ------------------------------------------- D3/D4 the bounds a caller can actually hit
# `index_search_scan_rows` (Task 13's fetch budget, 500 rows) only means something if a
# request can return that many; and `list_documents(since=...)` needs a format the
# server accepts, so the boundary values are measured, not assumed.
record("D3_documents_limit_bounds", {
    f"limit={value}": doc_summary(client.get("/api/documents", params={"limit": value}))
    for value in (2, 100, 101, 1000, 0, -5, "abc")
})
record("D4_documents_since_formats", {
    f"since={value!r}": doc_summary(client.get("/api/documents", params={"since": value}))
    for value in ("2026-11-17", "2026-11-17T22:28:05Z", "2026-11-17 22:28:05",
                  "1700000000", "not-a-date")
})
record("S7_search_page_beyond_and_limit_bounds", {
    "page=3&limit=2": {"params": {"q": "Rechnung", "page": 3, "limit": 2},
                       "status": search(q="Rechnung", page=3, limit=2).status_code,
                       "body": body_of(search(q="Rechnung", page=3, limit=2))},
    "limit=101": {"status": search(q="Rechnung", limit=101).status_code,
                  "echoed_limit": body_of(search(q="Rechnung", limit=101)).get("limit")},
    "page=0": {"status": search(q="Rechnung", page=0).status_code,
               "body": body_of(search(q="Rechnung", page=0))},
    "page=abc": {"status": search(q="Rechnung", page="abc").status_code,
                 "body": body_of(search(q="Rechnung", page="abc"))},
})

# ---------------------------------------------------------------- fixtures, from live bodies
written = []
search_body = body_of(with_hits)
search_ids = hit_ids(search_body)
if with_hits.status_code == 200 and search_ids and len(search_ids) == 1:
    written.append(write_fixture("kreuzakt_search_page.json", search_body))
else:
    print("REFUSING to write kreuzakt_search_page.json:", with_hits.status_code, search_ids)

if no_hits.status_code == 200 and hit_ids(body_of(no_hits)) == []:
    written.append(write_fixture("kreuzakt_search_empty.json", body_of(no_hits)))
else:
    print("REFUSING to write kreuzakt_search_empty.json:", no_hits.status_code)

if upload_first and upload_first.get("files"):
    written.append(write_fixture("kreuzakt_upload_response.json", upload_first))
else:
    print("REFUSING to write kreuzakt_upload_response.json")

if docs_default.status_code == 200 and hit_ids(default_body):
    written.append(write_fixture("kreuzakt_documents_page.json", default_body))
else:
    print("REFUSING to write kreuzakt_documents_page.json")

record("fixtures_written", written)
print("done")
