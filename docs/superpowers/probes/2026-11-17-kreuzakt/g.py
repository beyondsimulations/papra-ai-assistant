import json, time, urllib.request, hashlib, io
B="http://127.0.0.1:12400"
def req(path, method="GET", data=None, headers=None, raw=False):
    r=urllib.request.Request(B+path, data=data, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(r, timeout=60) as resp:
            body=resp.read()
            return resp.status, body if raw else (json.loads(body) if body else None)
    except urllib.error.HTTPError as e:
        body=e.read()
        try: return e.code, json.loads(body)
        except Exception: return e.code, body[:200].decode("utf8","replace")

# a tiny valid one-page PDF with the word StraBe in text layer
PDF=open("/tmp/kzprobe/sample.pdf","rb").read() if __import__("os").path.exists("/tmp/kzprobe/sample.pdf") else None
if PDF is None:
    import subprocess, sys
    subprocess.run([sys.executable,"-c","pass"],check=False)
    print("need sample.pdf"); raise SystemExit(1)

def upload(name, blob):
    bd="----kz"; parts=[]
    parts.append(f"--{bd}\r\nContent-Disposition: form-data; name=\"files\"; filename=\"{name}\"\r\nContent-Type: application/pdf\r\n\r\n".encode())
    parts.append(blob); parts.append(b"\r\n--"+bd.encode()+b"--\r\n")
    return req("/api/upload","POST", b"".join(parts), {"Content-Type":f"multipart/form-data; boundary={bd}"})

def wait_doc(path, tries=30):
    for _ in range(tries):
        s,q=req("/api/queue?limit=50")
        if s==200:
            rows=q.get("entries") or q.get("queue") or (q if isinstance(q,list) else [])
            for e in rows:
                if e.get("filename","").endswith(path) and e.get("status") in ("completed","failed"):
                    return e
        time.sleep(2)
    return None

print("health:", req("/api/health"))
print("\n== upload #1 (invoice-a.pdf) ==")
s,r=upload("invoice-a.pdf", PDF); print(s, json.dumps(r)[:400])
f1=(r.get("files") or [{}])[0]; print("upload row keys:", sorted(f1.keys()))
q1=wait_doc(f1["storedFilename"]); print("queue row:", json.dumps(q1)[:300])
did1=q1.get("document_id") if q1 else None
if did1:
    s,d=req(f"/api/documents/{did1}"); print("doc keys:", sorted(d.keys()) if s==200 else (s,d))
    print("fileHash:", str(d.get("fileHash"))[:16], "updatedAt:", d.get("updatedAt"), "content len:", len(d.get("content") or ""))

print("\n== upload #2: identical bytes, DIFFERENT filename (our two-bucket case) ==")
s,r=upload("bucket-b-invoice-a.pdf", PDF); print(s, json.dumps(r)[:400])
f2=(r.get("files") or [{}])[0]
q2=wait_doc(f2["storedFilename"]); print("queue row:", json.dumps(q2)[:300])
did2=q2.get("document_id") if q2 else None
print("doc1 id", did1, "doc2 id", did2, "-> same document?", (did1 is not None and did1==did2))

print("\n== upload #3: identical bytes, SAME filename ==")
s,r=upload("invoice-a.pdf", PDF); print(s, json.dumps(r)[:300])

print("\n== listing ==")
s,l=req("/api/documents?page=1&limit=10"); print(s, "keys:", sorted(l.keys()) if s==200 else l)
rows=l.get("documents") or l.get("items") or []
if rows: print("row keys:", sorted(rows[0].keys()))

print("\n== search ==")
s,res=req("/api/search?q=stra%20bei%20klanssi"); print(s, json.dumps(res)[:300])

print("\n== delete doc2/doc1 ==")
if did2: print("delete", did2, req(f"/api/documents/{did2}","DELETE"))
if did1 and did1!=did2: print("delete", did1, req(f"/api/documents/{did1}","DELETE"))
elif did1==did2 and did2: print("doc1 and doc2 share an id: deleting one deletes both (confirmed live)")
if did1: print("get after delete:", req(f"/api/documents/{did1}")[0])
