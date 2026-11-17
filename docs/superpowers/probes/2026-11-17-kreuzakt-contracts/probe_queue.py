"""Measure Kreuzakt's upload + queue contract against a real container.

Run against the Task 2 test stack:

    docker compose -f docker-compose.test.yml -p papra-ai-assistant stop
    docker compose -f docker-compose.test.yml -p kreuzakt-bridge up -d --wait
    .venv/bin/python docs/superpowers/probes/2026-11-17-kreuzakt-contracts/probe_queue.py

The container must be the pinned digest from docker-compose.test.yml. Keyless is
enough here: every question is queue/upload semantics, and keyless extraction of
a text-layer PDF is real extraction (measured 2026-11-17). Anything that would
need a configured OCR/LLM model is reported as such rather than guessed.

Answers are printed as `## Q< n >` sections and the raw payloads are appended to
`capture-queue.json` next to this file, so a quoted line in README.md can always
be traced to the bytes that produced it.

Deliberately NOT reformatting-clean: this file is measurement evidence, and
`docs/superpowers/probes/` is excluded from ruff in pyproject.toml so that the
quoted log and the script that produced it cannot drift apart.
"""

import json
import sys
import time
import urllib.parse
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).parent))
from make_pdf import build_pdf  # noqa: E402

BASE = "http://127.0.0.1:12400"
CAPTURE_PATH = Path(__file__).parent / "capture-queue.json"
capture = json.loads(CAPTURE_PATH.read_text()) if CAPTURE_PATH.exists() else {}

client = httpx.Client(base_url=BASE, timeout=60.0)
# Counted, not hand-summed: "total rows versus the number you uploaded" has to be
# defensible, and reindex-all also creates rows (one per document) that are not
# uploads.
accepted_uploads = [0]
reindex_rows_created = [0]


def record(key, value):
    capture[key] = value
    CAPTURE_PATH.write_text(json.dumps(capture, indent=2, default=str))
    print(json.dumps(value, indent=2, sort_keys=False)[:4000])


def queue(**params):
    return client.get("/api/queue", params=params or None).json()


def queue_rows(body):
    return body.get("entries") or []


def upload(name, blob):
    response = client.post("/api/upload", files={"files": (name, blob, "application/pdf")})
    if response.status_code == 201:
        accepted_uploads[0] += 1
    return response


def upload_many(pairs):
    response = client.post(
        "/api/upload",
        files=[("files", (name, blob, "application/pdf")) for name, blob in pairs],
    )
    if response.status_code == 201:
        accepted_uploads[0] += len(pairs)
    return response


def rows_for(filename):
    """Every visible queue row whose filename is exactly `filename`."""
    return [r for r in queue_rows(queue(limit=100)) if r["filename"] == filename]


def wait_terminal(filename, *, timeout_s=180.0, grace_s=0.0):
    """Block until a row named `filename` (exact match) reaches a terminal state."""
    time.sleep(grace_s)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for row in rows_for(filename):
            if row["status"] in ("completed", "failed"):
                return row
        time.sleep(0.4)
    return None


def doc(doc_id):
    response = client.get(f"/api/documents/{doc_id}")
    return response.status_code, response.json()


print("=" * 78)
print("Q0 health (which models this instance would use; keyless = no usable key)")
print("=" * 78)
record("Q0_health", client.get("/api/health").json())
print("baseline queue:", json.dumps(queue()))

# ---------------------------------------------------------------------------
# Q1: exact POST /api/upload responses.
# ---------------------------------------------------------------------------
print("\n" + "=" * 78)
print("Q1 upload response: one file, then two files in ONE request")
print("=" * 78)
one = upload("papra-doc0001-1a2b3c4d.pdf", build_pdf("Q1-SINGLE"))
record("Q1a_upload_one_file", {"status": one.status_code, "body": one.json()})
print("two files, one request:")
two = upload_many(
    [
        ("papra-doc0002-2b3c4d5e.pdf", build_pdf("Q1-TWO-A")),
        ("papra-doc0003-3c4d5e6f.pdf", build_pdf("Q1-TWO-B")),
    ]
)
record("Q1b_upload_two_files_one_request", {"status": two.status_code, "body": two.json()})
empty = client.post("/api/upload", files={"notFiles": ("x.pdf", build_pdf("Q1-EMPTY"), "application/pdf")})
record("Q1c_upload_wrong_field_name", {"status": empty.status_code, "body": empty.json()})

for name in ("papra-doc0001-1a2b3c4d.pdf", "papra-doc0002-2b3c4d5e.pdf", "papra-doc0003-3c4d5e6f.pdf"):
    row = wait_terminal(name)
    print(f"  {name} -> {row['status']} doc={row['document_id']} row={row['id']}" if row else f"  {name} -> timeout")

# ---------------------------------------------------------------------------
# Q6: what `filename` actually holds.
# ---------------------------------------------------------------------------
print("\n" + "=" * 78)
print("Q6 filename: upload name vs stored name vs both")
print("=" * 78)
# Three shapes at once: a plain bridge-style name, a name upload will sanitize,
# and a re-upload of a name that is already sitting in the ingest dir.
plain_name = "papra-doc0004-4d5e6f70.pdf"
plain_up = upload(plain_name, build_pdf("Q6-PLAIN"))
weird_name = "papra:doc0005<5e6f7081>.pdf"
weird_up = upload(weird_name, build_pdf("Q6-SANITIZED"))
dup_name = "papra-doc0006-6f708192.pdf"
dup_first = upload(dup_name, build_pdf("Q6-DUP-FIRST"))
dup_second = upload(dup_name, build_pdf("Q6-DUP-SECOND"))  # same name, file still in ingest
record(
    "Q6_filename_shapes",
    {
        "plain_upload": plain_up.json(),
        "sanitized_upload": weird_up.json(),
        "same_name_twice_first": dup_first.json(),
        "same_name_twice_second": dup_second.json(),
    },
)
time.sleep(6)  # let the ingest dir drain so the rows are terminal
record(
    "Q6_queue_rows_for_those_uploads",
    {
        "visible_rows": queue_rows(queue(limit=100)),
    },
)
doc_id_plain = next((r["document_id"] for r in rows_for(plain_name) if r["status"] == "completed"), None)
if doc_id_plain:
    status, body = doc(doc_id_plain)
    record("Q6_document_detail_for_plain_upload", {"status": status, "body": body})

# ---------------------------------------------------------------------------
# Q10: is `id` monotonic, and is the default ordering by id?
# ---------------------------------------------------------------------------
print("\n" + "=" * 78)
print("Q10 monotonic id + default ordering")
print("=" * 78)
# One multi-file request: three rows created inside the same second, so
# `created_at` (second granularity, SQLite datetime('now')) MUST tie. If the
# tie is resolved by id, ordering is provably id-ordered; if not, the
# high-water-mark rule dies.
same_second = upload_many(
    [
        ("papra-doc0010-0a0a0a0a.pdf", build_pdf("Q10-A")),
        ("papra-doc0011-0b0b0b0b.pdf", build_pdf("Q10-B")),
        ("papra-doc0012-0c0c0c0c.pdf", build_pdf("Q10-C")),
    ]
)
time.sleep(3)
page = queue(limit=100)
same_second_rows = sorted(
    [r for r in queue_rows(page) if r["filename"].startswith("papra-doc001")],
    key=lambda r: r["id"],
)
ids = [r["id"] for r in queue_rows(page)]
created = [r["created_at"] for r in queue_rows(page)]
strict_desc_ids = all(a > b for a, b in zip(ids, ids[1:]))
sorted_by_created_then_id = sorted(
    queue_rows(page), key=lambda r: (r["created_at"], r["id"]), reverse=True
)
ties = sum(1 for a, b in zip(created, created[1:]) if a == b)
record(
    "Q10_ordering",
    {
        "ids_in_returned_order": ids,
        "created_at_in_returned_order": created,
        "ids_strictly_descending": strict_desc_ids,
        "created_at_ties_adjacent": ties,
        "returned_order_equals_sort_created_at_desc_id_desc": [
            r["id"] for r in sorted_by_created_then_id
        ] == ids,
        "three_rows_same_request": same_second.json(),
        "the_three_rows_as_queue_entries": [
            {k: r[k] for k in ("id", "filename", "status", "created_at", "document_id")}
            for r in same_second_rows
        ],
        "three_rows_share_one_created_at": len({r["created_at"] for r in same_second_rows}) == 1,
        "returned_order_equals_sort_id_desc": ids == sorted(ids, reverse=True),
        "newest_row": queue_rows(page)[0] if ids else None,
    },
)
print(f"ids in returned order: {ids}")
print(f"ids strictly descending: {strict_desc_ids}; created_at adjacent ties: {ties}")

# Monotonicity in the direction the client needs: upload K files one at a time
# and read back the row id each upload produced, in upload order.
seq_names = [f"papra-seq{i:04d}-9e9e9e9e.pdf" for i in range(5)]
seq_ids = []
for name in seq_names:
    upload(name, build_pdf(f"SEQ-{name}"))
    time.sleep(1.2)
    found = rows_for(name)
    seq_ids.append(max(r["id"] for r in found) if found else None)
record(
    "Q10b_sequential_upload_ids",
    {
        "upload_order": seq_names,
        "row_ids_in_upload_order": seq_ids,
        "strictly_increasing": all(
            a < b for a, b in zip(seq_ids, seq_ids[1:]) if a is not None and b is not None
        ),
    },
)
print(f"row ids in upload order: {seq_ids}")

# ---------------------------------------------------------------------------
# Q8: rescan and reindex-all, measured while the corpus is still small.
# ---------------------------------------------------------------------------
print("\n" + "=" * 78)
print("Q8 rescan / reindex-all")
print("=" * 78)
target = doc_id_plain
if target is None:
    target = next((r["document_id"] for r in queue_rows(queue(limit=100)) if r["document_id"]), None)
before_status, before = doc(target)
counts_before = queue()["counts"]
rows_before = len(queue_rows(queue(limit=100)))
rescan = client.post(f"/api/documents/{target}/rescan")
time.sleep(3)
after_status, after = doc(target)
record(
    "Q8a_rescan",
    {
        "document_id": target,
        "before": {"status": before_status, "body": before},
        "response": {"status": rescan.status_code, "body": rescan.json()},
        "after": {"status": after_status, "body": after},
        "diff": {
            "id": (before.get("id"), after.get("id")),
            "updated_at": (before.get("updated_at"), after.get("updated_at")),
            "content_len": (len(before.get("content") or ""), len(after.get("content") or "")),
            "content_equal": before.get("content") == after.get("content"),
            "title": (before.get("title"), after.get("title")),
            "stored_filename": (before.get("stored_filename"), after.get("stored_filename")),
        },
        "queue_counts_before": counts_before,
        "queue_counts_after": queue()["counts"],
        "visible_rows_before": rows_before,
        "visible_rows_after": len(queue_rows(queue(limit=100))),
    },
)
print(json.dumps(capture["Q8a_rescan"]["diff"], indent=2))

reindex_post = client.post("/api/reindex-all")
reindex_status_1 = client.get("/api/reindex-all").json()
deadline = time.monotonic() + 240
while time.monotonic() < deadline:
    status = client.get("/api/reindex-all").json()
    if not status.get("status", {}).get("active"):
        break
    time.sleep(2)
reindex_rows = [r for r in queue_rows(queue(limit=100)) if r["filename"].startswith("reindex-all:")]
sample = reindex_rows[0] if reindex_rows else None
record(
    "Q8b_reindex_all",
    {
        "post": {"status": reindex_post.status_code, "body": reindex_post.json()},
        "status_while_running": reindex_status_1,
        "status_after": status,
        "reindex_rows_visible": len(reindex_rows),
        "reindex_row_sample": sample,
        "reindex_row_document_id_set_before_completion": (
            sample is not None and sample["status"] != "completed" and sample["document_id"] is not None
        ),
        "document_after_reindex": doc(target)[1],
    },
)
print(f"reindex rows visible: {len(reindex_rows)}; sample: {json.dumps(sample)[:400]}")
reindex_rows_created[0] = len(reindex_rows)

# ---------------------------------------------------------------------------
# Q2 duplicate bytes WHILE the first is still processing.
# ---------------------------------------------------------------------------
print("\n" + "=" * 78)
print("Q2 identical bytes uploaded twice while the first is still processing")
print("=" * 78)
bytes_a = build_pdf("Q2-SAME-BYTES")
first = upload("papra-doc0020-20202020.pdf", bytes_a)
mid = queue_rows(queue(limit=100))
mid_row = next((r for r in mid if r["filename"] == "papra-doc0020-20202020.pdf"), None)
second = upload("papra-doc0021-21212121.pdf", bytes_a)  # no wait: first still in flight
immediate = queue_rows(queue(limit=100))
record(
    "Q2_duplicate_while_processing",
    {
        "first_upload": {"status": first.status_code, "body": first.json()},
        "first_row_seen_midflight": mid_row,
        "second_upload": {"status": second.status_code, "body": second.json()},
        "both_rows_immediately_after": [
            r for r in immediate if r["filename"].startswith("papra-doc002")
        ],
    },
)
row_a = wait_terminal("papra-doc0020-20202020.pdf")
row_b = wait_terminal("papra-doc0021-21212121.pdf")
record(
    "Q2_duplicate_while_processing_final_rows",
    {
        "row_first": row_a,
        "row_second": row_b,
        "rows_named_first": rows_for("papra-doc0020-20202020.pdf"),
        "rows_named_second": rows_for("papra-doc0021-21212121.pdf"),
        "same_document_id": row_a["document_id"] == row_b["document_id"] if row_a and row_b else None,
    },
)
print(f"first  row: {json.dumps(row_a)}")
print(f"second row: {json.dumps(row_b)}")

# ---------------------------------------------------------------------------
# Q3 duplicate bytes AFTER completion.
# ---------------------------------------------------------------------------
print("\n" + "=" * 78)
print("Q3 identical bytes re-uploaded after completion")
print("=" * 78)
third = upload("papra-doc0022-22222222.pdf", bytes_a)
row_c = wait_terminal("papra-doc0022-22222222.pdf")
record(
    "Q3_duplicate_after_completion",
    {
        "upload": {"status": third.status_code, "body": third.json()},
        "row": row_c,
        "rows_named": rows_for("papra-doc0022-22222222.pdf"),
        "document_id_matches_the_first_two": (
            row_c["document_id"] == row_a["document_id"] if row_c and row_a else None
        ),
        "row_ids_so_far_for_the_same_bytes": sorted(
            r["id"]
            for r in queue_rows(queue(limit=100))
            if r["filename"].startswith("papra-doc002")
        ),
    },
)
print(f"third row: {json.dumps(row_c)}")

# same name again, now that the file has left the ingest dir
again = upload("papra-doc0020-20202020.pdf", bytes_a)
time.sleep(3)
record(
    "Q3b_same_name_reupload_after_completion",
    {
        "upload": {"status": again.status_code, "body": again.json()},
        "rows_named": rows_for("papra-doc0020-20202020.pdf"),
    },
)
print("rows now named papra-doc0020-20202020.pdf:", json.dumps(capture["Q3b_same_name_reupload_after_completion"]["rows_named"], indent=2)[:900])

# ---------------------------------------------------------------------------
# Q7 retry: new row or reused row?
# ---------------------------------------------------------------------------
print("\n" + "=" * 78)
print("Q7 POST /api/queue/{id}/retry")
print("=" * 78)
completed_row = row_a
retry_completed = client.post(f"/api/queue/{completed_row['id']}/retry")
record(
    "Q7a_retry_a_completed_row",
    {
        "request_id": completed_row["id"],
        "response": {"status": retry_completed.status_code, "body": retry_completed.json()},
        "rows_named_after": rows_for("papra-doc0020-20202020.pdf"),
    },
)
print("retry on a completed row:", json.dumps(capture["Q7a_retry_a_completed_row"]["response"]))

# need a failed row: a file whose extraction throws
bad = upload("papra-doc0030-30303030.pdf", b"%PDF-1.4\nthis is not a pdf, extraction must fail\n")
bad_row = wait_terminal("papra-doc0030-30303030.pdf")
print("garbage upload row:", json.dumps(bad_row))
if bad_row and bad_row["status"] == "failed":
    ids_before = sorted(r["id"] for r in queue_rows(queue(limit=100)))
    counts_before = queue()["counts"]
    retry_failed = client.post(f"/api/queue/{bad_row['id']}/retry")
    time.sleep(4)
    rows_after = rows_for("papra-doc0030-30303030.pdf")
    ids_after = sorted(r["id"] for r in queue_rows(queue(limit=100)))
    record(
        "Q7b_retry_a_failed_row",
        {
            "failed_row": bad_row,
            "response": {"status": retry_failed.status_code, "body": retry_failed.json()},
            "row_ids_before": ids_before,
            "row_ids_after": ids_after,
            "new_row_created": sorted(set(ids_after) - set(ids_before)),
            "counts_before": counts_before,
            "counts_after": queue()["counts"],
            "rows_named_after": rows_after,
        },
    )
    print(json.dumps(capture["Q7b_retry_a_failed_row"], indent=2)[:2000])
else:
    record("Q7b_retry_a_failed_row", {"note": "could not produce a failed row", "row": bad_row})

missing = client.post("/api/queue/999999/retry")
record("Q7c_retry_unknown_id", {"status": missing.status_code, "body": missing.json()})

# ---------------------------------------------------------------------------
# Q9 can a completed row have document_id = null?
# ---------------------------------------------------------------------------
print("\n" + "=" * 78)
print("Q9 document_id null on a completed row")
print("=" * 78)
victim = row_a["document_id"]
rows_of_victim_before = [r for r in queue_rows(queue(limit=100)) if r["document_id"] == victim]
delete = client.delete(f"/api/documents/{victim}")
time.sleep(1)
rows_of_victim_after = [r for r in queue_rows(queue(limit=100)) if r["filename"] in
                        ("papra-doc0020-20202020.pdf", "papra-doc0021-21212121.pdf", "papra-doc0022-22222222.pdf")]
record(
    "Q9_null_document_id_on_completed",
    {
        "deleted_document_id": victim,
        "delete_status": delete.status_code,
        "rows_pointing_at_it_before": rows_of_victim_before,
        "those_rows_after_delete": rows_of_victim_after,
        "completed_rows_with_null_document_id": [
            r for r in queue_rows(queue(limit=100))
            if r["status"] == "completed" and r["document_id"] is None
        ],
        "document_get_after_delete": doc(victim)[0],
    },
)
print(json.dumps(capture["Q9_null_document_id_on_completed"], indent=2)[:2500])

# ---------------------------------------------------------------------------
# Fixtures for Tasks 5/6/13, written verbatim from these responses.
#
# The probe writes them instead of a human transcribing them: a guessed field
# name then cannot survive into the suite. `limit=100` is the server's own cap,
# and this is the moment the queue holds every interesting row shape at once —
# completed with an id, completed with document_id nulled by the delete above,
# and a failed row with its error text.
# ---------------------------------------------------------------------------
print("\n" + "=" * 78)
print("fixtures: tests/fixtures/kreuzakt_queue_page.json + kreuzakt_document.json")
print("=" * 78)
fixtures_dir = Path(__file__).resolve().parents[4] / "tests" / "fixtures"
fixtures_dir.mkdir(parents=True, exist_ok=True)
page_payload = queue(limit=100)
fixtures_dir.joinpath("kreuzakt_queue_page.json").write_text(
    json.dumps(page_payload, indent=2) + "\n"
)
live = [
    r["document_id"]
    for r in queue_rows(page_payload)
    if r["status"] == "completed" and r["document_id"] not in (None, victim)
]
document_status, document_payload = doc(live[0])
fixtures_dir.joinpath("kreuzakt_document.json").write_text(
    json.dumps(document_payload, indent=2) + "\n"
)
record(
    "fixtures_written",
    {
        "queue_page_source": "GET /api/queue?limit=100",
        "queue_page_rows": len(queue_rows(page_payload)),
        "queue_page_counts": page_payload["counts"],
        "document_source": f"GET /api/documents/{live[0]}",
        "document_status": document_status,
        "document_keys": sorted(document_payload.keys()),
    },
)
print("wrote", fixtures_dir)

# ---------------------------------------------------------------------------
# Q4/Q5: pagination, caps, retention. Done last and with enough rows that a
# limit is actually visible: uploads create rows immediately (status pending),
# so row count is known exactly without waiting for extraction.
# ---------------------------------------------------------------------------
print("\n" + "=" * 78)
print("Q4/Q5 accepted parameters, largest limit, walk-until-empty, retention")
print("=" * 78)
uploads_before = queue()["counts"]
total_uploads_before = sum(uploads_before.values())
mass = []
n = 120
for i in range(n):
    mass.append((f"papra-mass{i:04d}-ffffffff.pdf", build_pdf(f"MASS-{i:04d}")))
mass_responses = []
for chunk_start in range(0, len(mass), 20):
    response = upload_many(mass[chunk_start : chunk_start + 20])
    mass_responses.append(response.json())
expected_rows = total_uploads_before + n
print(f"uploaded {n} more files; expected total queue rows {expected_rows}")

probe_params = [
    {},
    {"limit": 1},
    {"limit": 5},
    {"limit": 50},
    {"limit": 100},
    {"limit": 101},
    {"limit": 500},
    {"limit": 10000},
    {"limit": 0},
    {"limit": -5},
    {"limit": "abc"},
    {"page": 2, "limit": 5},
    {"page": 3, "limit": 5},
    {"offset": 5, "limit": 5},
    {"offset": 10, "limit": 5},
    {"page": 2, "offset": 5, "limit": 5},
    {"status": "completed", "limit": 5},
    {"status": "pending", "limit": 5},
    {"status": "failed", "limit": 5},
    {"status": "bogus", "limit": 5},
    {"perPage": 5},
    {"per_page": 5},
    {"count": 5},
    {"after": 0},
    {"cursor": "abc"},
]
param_results = {}
for params in probe_params:
    response = client.get("/api/queue", params=params)
    body = response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text
    rows = queue_rows(body) if isinstance(body, dict) else []
    key = urllib.parse.urlencode(params) or "(no params)"
    param_results[key] = {
        "status": response.status_code,
        "rows_returned": len(rows),
        "first_ids": [r["id"] for r in rows[:5]],
        "counts": body.get("counts") if isinstance(body, dict) else None,
        "body_if_not_entries": None if isinstance(body, dict) and "entries" in body else body,
    }
record("Q4_parameter_matrix", param_results)
for key, value in param_results.items():
    print(f"  ?{key:34s} -> {value['status']} rows={value['rows_returned']} ids={value['first_ids']}")

# walk-until-empty attempts: by page, then by offset
def all_reachable(params):
    """Every row the endpoint will give us for a param set, capped by the server cap."""
    return {r["id"]: r for r in queue_rows(queue(**params))}


def walk_by_page(limit):
    seen, pages, first_page = [], 1, None
    while pages <= 40:
        rows = queue_rows(queue(limit=limit, page=pages))
        if first_page is None:
            first_page = [r["id"] for r in rows]
        elif [r["id"] for r in rows] == first_page:
            return {"stop": "page 2..N returned the identical id set as page 1", "pages": pages, "distinct_ids": len(set(seen))}
        if not rows:
            return {"stop": "empty page", "pages": pages, "distinct_ids": len(set(seen))}
        seen.extend(r["id"] for r in rows)
        pages += 1
    return {"stop": "hit 40-page fuse", "pages": pages - 1, "distinct_ids": len(set(seen))}


def walk_by_offset(limit):
    seen, offset, first = set(), 0, None
    while offset <= 2000:
        rows = queue_rows(queue(limit=limit, offset=offset))
        ids = [r["id"] for r in rows]
        if first is None:
            first = ids
        if not rows:
            return {"stop": "empty page", "offset": offset, "distinct_ids": len(seen)}
        if offset and ids == first:
            return {"stop": "offset ignored: same ids as offset=0", "offset": offset, "distinct_ids": len(seen)}
        seen.update(ids)
        offset += limit
    return {"stop": "hit offset fuse", "distinct_ids": len(seen)}


walk = {"by_page_limit_100": walk_by_page(100), "by_offset_limit_100": walk_by_offset(100)}
counts_now = queue()["counts"]
reachable = len(queue_rows(queue(limit=100)))

# Can a row the default window hides be reached at all? `status` is the only
# other filter the endpoint accepts, so it is the only candidate lever.
by_status = {s: sorted(all_reachable({"status": s, "limit": 100})) for s in
             ("pending", "processing", "completed", "failed")}
union = set()
for ids in by_status.values():
    union |= set(ids)
record(
    "Q4_status_window",
    {
        "ids_per_status": {k: [v[0], v[-1]] if v else None for k, v in by_status.items()},
        "rows_per_status": {k: len(v) for k, v in by_status.items()},
        "counts": counts_now,
        "union_of_status_windows": len(union),
        "total_rows_per_counts": sum(counts_now.values()),
        "default_window_hides_older_than_id": min(all_reachable({"limit": 100})),
    },
)
print(json.dumps(capture["Q4_status_window"], indent=2))

# Is a row we uploaded reachable while a burst of newer rows sits on top of it?
# papra-doc0001 is the very first row this probe created (id 1).
correlation_window = {
    "looked_for": "papra-doc0001-1a2b3c4d.pdf",
    "found_in_default_window": bool(rows_for("papra-doc0001-1a2b3c4d.pdf")),
    "found_via_status_filter": bool(
        [r for r in queue_rows(queue(status="completed", limit=100))
         if r["filename"] == "papra-doc0001-1a2b3c4d.pdf"]
    ),
    "rows_newer_than_it": sum(counts_now.values()) - 1,
    "high_water_mark_from_limit_1": queue_rows(queue(limit=1))[0],
}
record("Q4_correlation_window", correlation_window)
print(json.dumps(correlation_window, indent=2))

record(
    "Q4_walk",
    {
        **walk,
        "rows_reachable_at_limit_100": reachable,
        "rows_reachable_at_limit_10000": len(queue_rows(queue(limit=10000))),
        "reachable_ids_are_the_highest_100": sorted(all_reachable({"limit": 100})) == sorted(
            all_reachable({"limit": 100}))[-100:],
        "counts": counts_now,
        "total_rows_per_counts": sum(counts_now.values()),
        "uploads_accepted_by_this_probe": accepted_uploads[0],
        "reindex_rows_created_by_this_probe": reindex_rows_created[0],
        "rows_explained": accepted_uploads[0] + reindex_rows_created[0],
        "expected_rows_after_mass_upload": expected_rows,
    },
)
print(json.dumps(capture["Q4_walk"], indent=2))

# retention: do old rows disappear as new ones arrive?
oldest = client.get("/api/queue", params={"limit": 100})
oldest_rows = queue_rows(oldest.json())
record(
    "Q5_retention",
    {
        "counts": queue()["counts"],
        "lowest_visible_id": min(r["id"] for r in oldest_rows),
        "highest_visible_id": max(r["id"] for r in oldest_rows),
        "row_id_1_still_reachable": any(r["id"] == 1 for r in oldest_rows),
        "note": "limit is capped, so reachability is the question, not deletion",
    },
)
print(json.dumps(capture["Q5_retention"], indent=2))

print("\ncaptures written to", CAPTURE_PATH)
client.close()