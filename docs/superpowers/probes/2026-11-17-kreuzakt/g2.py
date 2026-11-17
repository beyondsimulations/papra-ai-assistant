import json, time, urllib.request, urllib.error, urllib.parse
B="http://127.0.0.1:12400"
def req(path, method="GET", data=None, headers=None):
    r=urllib.request.Request(B+path, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(r, timeout=60) as resp:
            b=resp.read()
            return resp.status, (json.loads(b) if b else None)
    except urllib.error.HTTPError as e:
        b=e.read()
        try: return e.code, json.loads(b)
        except Exception: return e.code, b[:160].decode("utf8","replace")
PDF=open("/tmp/kzprobe/sample.pdf","rb").read()
def upload(name):
    bd="----kz"
    body=(f"--{bd}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"{name}\"\r\nContent-Type: application/pdf\r\n\r\n").encode()+PDF+(b"\r\n--"+bd.encode()+b"--\r\n")
    return req("/api/upload","POST",body,{"Content-Type":f"multipart/form-data; boundary={bd}"})

print("== upload with a unique, bridge-style name ==")
s,r=upload("papra-doc9001-a1b2c3d4.pdf"); print(s, json.dumps(r))
rel=r["files"][0]["relativePath"]
for _ in range(20):
    s,q=req("/api/queue?limit=50")
    rows=q.get("entries") or q.get("items") or (q if isinstance(q,list) else [])
    hit=[e for e in rows if e.get("filename","").endswith(rel)]
    if hit and hit[0]["status"] in ("completed","failed"): break
    time.sleep(1.5)
print("queue keys:", sorted(hit[0].keys()), "status:", hit[0]["status"], "doc:", hit[0]["document_id"], "error:", hit[0]["error"])
did=hit[0]["document_id"]

s,d=req(f"/api/documents/{did}")
print("\n== detail: title/description/date/version markers ==")
for k in ("title","description","document_date","added_at","created_at","updated_at","stored_filename","original_filename","page_count","file_size","download_url","link_for_user","thumbnail_url"):
    print(f"  {k} = {str(d.get(k))[:70]}")
print("  content =", repr((d.get("content") or "")[:60]))

print("\n== positive search (terms from the PDF text layer) ==")
for q in ("rechnung", "strasse", "miete"):
    s,res=req("/api/search?q="+urllib.parse.quote(q)); h=(res or {}).get("results") or []
    print(f"  q={q!r} total={res.get('total')} hits={len(h)}", "hit keys:", sorted(h[0].keys()) if h else None)
    if h: print("     score:", h[0].get("score"), "snippet:", repr(str(h[0].get("content_snippet"))[:70]))

print("\n== paging bounds on /api/search ==")
for lim in (1, 100, 500):
    s,res=req("/api/search?q=rechnung&limit=%d"%lim); print(f"  limit={lim} -> status {s}, effective limit={res.get('limit') if isinstance(res,dict) else res}")
s,res=req("/api/search?q=rechnung&limit=500&offset=1"); print("  offset accepted?", s, res.get("limit") if isinstance(res,dict) else res)

print("\n== /api/documents?since incremental ==")
s,l=req("/api/documents?page=1&limit=5"); print("  total:", l.get("total") if isinstance(l,dict) else l)
s,l=req("/api/documents?since=2020-01-01"); print("  since=2020 ->", s, l.get("total") if isinstance(l,dict) else l)
s,l=req("/api/documents?since=2030-01-01"); print("  since=2030 ->", s, l.get("total") if isinstance(l,dict) else l)

print("\n== delete then re-upload (id reuse?) ==")
print("  delete:", req(f"/api/documents/{did}","DELETE")[0])
s,r=upload("papra-doc9001-a1b2c3d4.pdf"); rel2=r["files"][0]["relativePath"]
print("  re-upload relativePath:", rel2, "(same path as before?", rel2==rel, ")")
for _ in range(20):
    s,q=req("/api/queue?limit=50"); rows=q.get("entries") or []
    hit=[e for e in rows if e.get("filename","").endswith(rel2)]
    if hit and hit[0]["status"] in ("completed","failed"): break
    time.sleep(1.5)
newid=hit[0]["document_id"] if hit else None
print("  new document id:", newid, "vs old", did, "-> id reused?" , newid==did)
print("\n== queue rows accumulate per upload (retention) ==")
s,q=req("/api/queue?limit=100"); rows=q.get("entries") or []
print("  queue rows:", len(rows), "same-filename rows for our name:", sum(1 for e in rows if "doc9001" in (e.get("filename") or "")))
