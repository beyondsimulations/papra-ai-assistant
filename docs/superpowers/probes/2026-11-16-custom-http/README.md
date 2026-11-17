# Papra content extraction: defaults, quality, the write race, and `custom-http` (2026-11-16)

These measurements settle whether Papra's own content extraction may be the final writer of
the `content` column — the open content-write-race decision. Measured against the pinned
image `ghcr.io/papra-hq/papra@sha256:d1f596a35332ad04d8a69d88a5e19bc82acd47f2bc2fe687dd2145b1ef5a8b19`
(the digest `docker-compose.test.yml` pins). The scratch stack stays on port 12210; every
other arm runs in its own throwaway container, so no measurement here depends on wiping
anything.

## Corrections to claims made earlier in this directory

Both were produced by this author and both changed a conclusion, so they are recorded rather
than quietly dropped:

1. **The scans in `../2026-11-16/` had a text layer.** They were built with `insert_text`, so
   nothing in them could say anything about image-only input. Every fixture here asserts
   `page.get_text().strip() == ""` before uploading.
2. **`CONTENT_EXTRACTION_ENABLED` does not exist.** It came out of
   `grep -o "CONTENT_EXTRACTION[A-Z_]*"`, which matched the tail of a longer identifier. The
   real variable is `DOCUMENTS_CONTENT_EXTRACTION_ENABLED`, found by whole-identifier
   matching and then confirmed behaviourally in probe C2. The `CONTENT_EXTRACTION_CUSTOM_HTTP_*`
   names are real and were additionally observed on the wire (probe B).
3. **A first run of probe C patched the document at +5 s** — after Papra's extraction had
   already settled — and both arms looked healthy. That run proved nothing and is superseded
   by `probe_c2_exact.py`.

## 1. Papra's default strategy is real OCR, configured with English language data only

`probe_a_defaults.py` (upload, touch nothing, read what Papra wrote): a 200 dpi image-only
scan and a text-layer PDF both came back with the exact text, detected stable within 3.1 s
(that number includes a deliberate 2 s stability confirmation, so it bounds detection, not
OCR runtime), and content search found a token that exists only inside the image. The image
ships `/app/eng.traineddata`, so `internal` includes Tesseract.

`probe_a2_harder_inputs.py` is the measurement that matters, because the corpus is German.
One org per fixture, assertions on the fixture's own document id, and no ASCII rendering of
any tested word on the page (an earlier version planted control lines, and those control
lines were what the "hit" counts were actually matching).

Page printed with `Die Wäsche Straße Grüße Café Köln`, image-only, 200 dpi. Extracted:

```
Rechnung QRZ7UMWAESCHE / Die Wasche StraBe GriiBe Café Kdln / Betrag: 1.234,56 EUR
```

| query | this document | why |
| --- | --- | --- |
| `Wäsche` | **hit** | content says `Wasche`; query-time folding covers the umlaut |
| `Wasche` | hit | literal |
| `Waesche` | miss | `ae` is not expanded |
| `Straße` | **miss** | content says `StraBe` |
| `Strasse` | **miss** | ditto |
| `Grüße` / `Grusse` | **miss** | content says `GriiBe` |
| `Café` / `Cafe` | hit | `é` survived and folds |
| `Köln` / `Koln` | **miss** | content says `Kdln` — `ö` misread as `d`, so folding cannot rescue it |

Degraded fixture (JPEG q45 plus halved resolution), page printed with `Die Wäsche geht durch
die Straße`: extracted `Die Wasche geht durch die StraB3e`; `Wäsche`/`Wasche` hit, `Straße`
and `Strasse` both **miss**.

Three things follow, scoped to what was actually measured:

- Umlauts are folded at query time (`ä`→`a`, `é` survives), but `ß` is not folded and `ae`/`ue`
  are not expanded. So the failure is unrecoverable exactly where the letter was `ß`.
- The `ß` mangle is **not stable**: across three runs the same word came out `StraBe`,
  `StraB3e`, and `StraBBe`. A fixed search-time substitution is therefore not a workaround.
- Image quality was not the cause — the degraded page extracted fine apart from the same
  German characters. No rotation/perspective fixture was possible without a new dependency,
  and that limitation is in the script's docstring.

Scope limit, stated rather than glossed: `DOCUMENTS_OCR_LANGUAGES` exists and only
`eng.traineddata` is in the image, so German quality is probably improvable by mounting
`deu.traineddata`. Untested here, and either way it does not remove the write race in §2 —
it only changes which text loses.

## 2. Disabling Papra's extraction makes our write durable — one variable apart

`probe_c2_exact.py`, run against two containers. Both upload the same image-only scan and
`PATCH(content)` immediately after upload (the timing the OCR worker really produces). The
image carries `QRZ7IMAGEONLY`; our text carries `QRZ7PATCHONLY`, which exists on no pixel of
the page — so a hit on it can only mean our text is what is indexed. Comparison is exact
text, not length.

| arm | exact-text-equal-to-ours at t+0,2,5,10,20,30,45,60,90 s | `QRZ7PATCHONLY` | `QRZ7IMAGEONLY` | `Straße` |
| --- | --- | --- | --- | --- |
| `DOCUMENTS_CONTENT_EXTRACTION_ENABLED=false` (12211) | True at all 9 samples | **hit** | miss | **hit** |
| default, extraction on (12210) | True at t+0, **False** from t+2 on | **miss** | hit | **miss** |

In the control arm Papra's extraction lands between t+0 and t+2, once, and it wins: the
final content is Papra's own OCR text and our text is gone. The disabled arm stores our text
byte-exact for the whole 90 s window, `QRZ7IMAGEONLY` never becomes findable (Papra extracted
nothing), and `Straße` stays findable because our model spelled it correctly.

The control arm also shows why the token split was necessary: there, content search for
`Wäsche` returned a **hit** even though our text was gone — Papra's own OCR wrote `Wasche`
and the query folds. **A content-search hit is not evidence that our text survived.**

Reproduce an arm:

```bash
docker run -d --name papra-probe-c -p 127.0.0.1:12211:1221 \
  -e AUTH_SECRET='probe-secret-0123456789abcdef0123456789abcdef' \
  -e AUTH_IS_EMAIL_VERIFICATION_REQUIRED=false -e AUTH_IS_REGISTRATION_ENABLED=true \
  -e DOCUMENTS_CONTENT_EXTRACTION_ENABLED=false \
  ghcr.io/papra-hq/papra@sha256:d1f596a35332ad04d8a69d88a5e19bc82acd47f2bc2fe687dd2145b1ef5a8b19
```

## 3. The `custom-http` contract, and what happens when we are the slow component

Container env: `CONTENT_EXTRACTION_STRATEGY=custom-http`,
`..._CUSTOM_HTTP_URL=http://host.docker.internal:12399/extract`,
`..._RESPONSE_FORMAT=text`, `..._UPLOAD_FORMAT=form-data`,
`..._REQUEST_TIMEOUT_MS=5000`. Catch server on the host (`probe_b_custom_http.py`).

What Papra sends, for every upload:

```
POST /extract
content-type: multipart/form-data; boundary=----formdata-undici-…
headers: accept, accept-encoding, accept-language, connection, content-type, host, user-agent
body: name="file"; filename="text-ok.pdf"; Content-Type: application/pdf; 11,600,080 bytes
```

- **Extraction is asynchronous**: the upload returned in **0.1 s**, so a slow local VLM does
  not slow down intake.
- **No credential is sent.** Authentication has to come from
  `CONTENT_EXTRACTION_CUSTOM_HTTP_HEADERS`, and the listener has to be reachable from the
  container (tailnet-bound in production, never Funnel).
- **A plain-text response body becomes `content` verbatim** (42 chars returned, 42 stored).

`probe_b2_retry.py` then watched each failing case for a full 180 s, sampling at
t+0,5,15,30,60,120,180 and reporting the end of the window explicitly:

| our endpoint | extraction requests over 180 s | content at end of window |
| --- | --- | --- |
| `HTTP 500` | **1** | `''` |
| answers at 12 s, past the 5 s timeout | **1** | `''` |

So within a 3-minute window there is no retry, and **a response that lands after the client
timeout is discarded rather than stored**. Whether a retry exists at some longer interval was
not measured, and nothing here says the document ever gets its text back on its own.

The consequence for design: with a local VLM, "slow" is the normal failure mode, not a
corner case — and `custom-http` turns it into a document with no content and no signal. Any
`custom-http` design needs our own sweeper for documents still empty after a bounded window.
That is only safe *because* a late answer is discarded and no retry arrived in 180 s; it is
not a proof of a bounded extraction lifetime, and the spec would have to say what window it
assumes and why.

## 4. What our own code cannot currently recover (this is the real gate)

Read from `papra_suite/pipeline.py` and `papra_suite/store.py` while checking whether
"disable Papra's extraction" is self-sufficient. It is not, on today's code:

- `enqueue_ocr_job` deliberately enqueues **before** recording the document as seen, so a
  crash before the enqueue is recovered by the sweep. ✓
- A job claimed and abandoned by a crash is rewound to pending at startup. ✓
- `worker_loop` fails a job **permanently** on any 4xx except 429, or on
  `error.permanent` (a truncated completion), and otherwise backs off for 3 attempts. After
  that the job is `failed`.
- **`sweep_once` skips documents already in the seen table**, and the seen row is written as
  soon as intake succeeds — long before content is confirmed. So a document whose OCR job
  failed permanently has no content, and nothing re-discovers it. The only path back is the
  manual `reset_job`.
- A 409 "already ingested" from upload does not enqueue OCR, so mail replay cannot repair it
  either.

Today Papra's own extraction silently papers over that hole (badly, per §1). Turning it off
removes the papering-over, which is the point of the option — so the option has to arrive
with a recovery path for "document exists, content empty", or it converts a wrong-text bug
into a no-text bug.

## 5. What these measurements support, and what they do not

Supported:

- `DOCUMENTS_CONTENT_EXTRACTION_ENABLED=false` eliminates the second writer outright — one
  environment variable, no fork, no new listener, no reconciliation — and the stored text is
  then exactly what our model produced (§2).
- Letting Papra's extraction win is not neutral for a German corpus: `Straße`, `Grüße` and
  `Köln` are all unfindable afterwards, and the mangle varies run to run (§1).
- `custom-http` is the option for keeping Papra-native intake working, and its price is an
  authenticated tailnet listener plus a bounded-window sweeper (§3).

Not supported, and needing measurement or a decision before a spec:

- Whether `deu.traineddata` + `DOCUMENTS_OCR_LANGUAGES` makes Papra's own text good enough
  (untested; still leaves the race).
- What else `DOCUMENTS_CONTENT_EXTRACTION_ENABLED=false` changes in the product — only
  `content` and content-search were verified.
- Any claim about extraction behaviour beyond 180 s.
- A real photograph: no rotation/perspective/skew fixture exists, because building one would
  add a dependency to a probe.
- The recovery gap in §4, which is a code change and needs the failure cases
  (crash-before-enqueue, PATCH failure, PATCH-response-lost, worker death, exhausted job,
  409-replay) enumerated and tested against a restart.
