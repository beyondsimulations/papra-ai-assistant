# Papra content extraction: defaults, quality, and the `custom-http` contract (2026-11-16)

These measurements settle whether Papra's own content extraction can be allowed to be the
final writer of the `content` column, which is what the open content-write-race decision
turned on. Run with the scratch stack (`docker compose -f docker-compose.test.yml up -d`)
plus one throwaway container per arm; nothing here writes to the project's own state.

Everything below is measured against the pinned image
`ghcr.io/papra-hq/papra@sha256:d1f596a35332ad04d8a69d88a5e19bc82acd47f2bc2fe687dd2145b1ef5a8b19`
(the digest `docker-compose.test.yml` pins). Readiness: `/api/health`, ~3 s from `docker run`.

## Correction to the earlier probes in this directory

Two claims from `../2026-11-16/README.md` were produced by this author and were wrong in
ways that mattered, so they are corrected here rather than left standing:

1. **The "scans" had a text layer.** They were built with `insert_text` into a page, so
   every conclusion drawn from them about *image-only* input was unsupported. The scripts
   here assert `page.get_text().strip() == ""` before uploading, and the assertion is the
   point: without it, a fixture is a text-layer PDF wearing a costume.
2. **The `custom-http` env names were mis-transcribed from a substring grep.**
   `grep -o "CONTENT_EXTRACTION[A-Z_]*"` matched the *tail* of a longer identifier, so
   `CONTENT_EXTRACTION_ENABLED` was reported as an env var that does not exist. The real
   switch is `DOCUMENTS_CONTENT_EXTRACTION_ENABLED`. Whole-identifier matching
   (`grep -oa "[A-Z][A-Z0-9_]*CONTENT_EXTRACTION[A-Z0-9_]*"`) was used here instead, and
   the `CUSTOM_HTTP` names below were then confirmed on the wire by probe B.

## 1. Papra's default extraction is real OCR, and it is English-only

`probe_a_defaults.py` — upload, touch nothing, read what Papra wrote:

| input | Papra's `content` | settled | content search for a token only in the image |
| --- | --- | --- | --- |
| image-only scan, 200 dpi | 53 chars, exact text | 3.1 s | hit (`documentsCount=1`) |
| text-layer PDF | 53 chars, exact text | 3.1 s | hit |

So the default `internal` strategy OCRs: the image ships `eng.traineddata` (Tesseract).
A clean rendered page is transcribed perfectly in about three seconds. That is the easiest
input an OCR ever sees, which is why it is not the measurement that decides anything.

`probe_a2_harder_inputs.py` — same strategy, harder input. Page printed with
`Die Wäsche Straße Grüße Café Köln`, image-only, 200 dpi:

```
extracted: '...Die Wasche StraBe GriiBe Café Koln...'
search 'Wäsche' -> 1   search 'Straße' -> 0   search 'Grüße' -> 0
search 'Wasche' -> 1   search 'Strasse' -> 1  search 'Grusse' -> 0
search 'Café'   -> 1   search 'Köln'   -> 1
```

Three things, all relevant to a German corpus:

- The English model drops umlauts (`Wäsche`→`Wasche`), misreads `ß` as `B`
  (`Straße`→`StraBe`), and produces garbage on `ü`+`ß` (`Grüße`→`GriiBe`).
- Papra's query-time normalisation **does** fold umlauts (`Wäsche` finds `Wasche`) but
  **does not** fold `ß` and does not expand `ae`/`ue`. So `Straße` and `Grüße` return
  **zero documents**: the mangle is not recoverable at query time.
- A degraded fixture (JPEG q45, halved resolution) still extracted correctly, so the
  failure is the language model, not image quality. No rotation/perspective test was
  possible without a new dependency, and that limitation is in the script's docstring.

Consequence: letting Papra's extraction be the final writer is not a neutral choice for
this corpus. It is a choice to make `Straße` unfindable.

## 2. `DOCUMENTS_CONTENT_EXTRACTION_ENABLED=false` makes our write durable

`probe_c_disabled.py`, run against two arms that differ in one environment variable,
uploading the same image-only scan and patching our own text immediately after upload
(the timing the OCR worker actually produces, and the timing that loses the race):

| arm | content right after upload | content timeline after our PATCH | our text survives | search `Straße` |
| --- | --- | --- | --- | --- |
| `DOCUMENTS_CONTENT_EXTRACTION_ENABLED=false` (port 12211) | `0 chars` | `60,60,60,60,60,60,60,60` over 60 s | **yes** | **1** |
| default, extraction on (port 12210) | `0 chars` | `60,63,63,63,63,63,63,63` | **no** | **0** |

`63` is Papra's own extraction landing on top of our `60`. It arrives within ~2 s of the
upload, once, and it wins. Under the disabled arm Papra never writes the column at all, so
our `PATCH` is the only write and search reflects exactly the text our model produced —
including a correct `ß`.

Note the trap in the control arm: the content search for the token still returned 1 there,
because Papra's OCR read the token off the image itself. **A content-search hit is not
evidence that our text survived** — the first run of this probe was patched at +5 s, after
extraction had already settled, and both arms looked fine. Timing is the variable.

Reproduce an arm with:

```bash
docker run -d --name papra-probe-c -p 127.0.0.1:12211:1221 \
  -e AUTH_SECRET='probe-secret-0123456789abcdef0123456789abcdef' \
  -e AUTH_IS_EMAIL_VERIFICATION_REQUIRED=false -e AUTH_IS_REGISTRATION_ENABLED=true \
  -e DOCUMENTS_CONTENT_EXTRACTION_ENABLED=false \
  ghcr.io/papra-hq/papra@sha256:d1f596a35332ad04d8a69d88a5e19bc82acd47f2bc2fe687dd2145b1ef5a8b19
```

## 3. The `custom-http` contract, read off the wire

`probe_b_custom_http.py` ran a catch server on the host (port 12399) and a container with
`CONTENT_EXTRACTION_STRATEGY=custom-http`, `..._CUSTOM_HTTP_URL=http://host.docker.internal:12399/extract`,
`..._RESPONSE_FORMAT=text`, `..._UPLOAD_FORMAT=form-data`, `..._REQUEST_TIMEOUT_MS=5000`.

What Papra sends, for every upload:

```
POST /extract
content-type: multipart/form-data; boundary=----formdata-undici-…
headers: accept, accept-encoding, accept-language, connection, content-type, host, user-agent
body: name="file"; filename="text-ok.pdf"; Content-Type: application/pdf; 11,600,080 bytes
```

- **Asynchronous**: the upload returned in **0.1 s**, so extraction latency never delays
  intake — our VLM can be slow without the mail poller noticing.
- **No credential is sent by default.** The only auth is
  `CONTENT_EXTRACTION_CUSTOM_HTTP_HEADERS`, which must carry a shared secret, and the
  listener must be reachable from the container (tailnet-bound in production, never Funnel).
- **Plain-text response body becomes `content` verbatim** (42 chars returned → 42 chars stored).

Failure behaviour, and this is the reason `custom-http` is the fallback and not the answer:

| our endpoint | requests received | final `content` |
| --- | --- | --- |
| `500` | **1**, no retry | `''` — empty, permanently |
| hangs past the 5 s timeout | **1**, no retry | `''` — empty, permanently |

No retry, no error surfaced on the document, no extraction-status field to poll. A document
uploaded while our suite is down, restarting, or queue-saturated silently keeps no content
forever, and nothing in Papra says so. Any design built on `custom-http` therefore needs a
sweeper that patches documents still empty after a bounded window — which is only safe
*because* there is no retry, so "empty at T+window" is a final state rather than a race.

## What these measurements support

- **Disabling Papra's extraction** (`DOCUMENTS_CONTENT_EXTRACTION_ENABLED=false`) removes
  the race outright: one env var, no fork, no new listener, no reconciliation, and the
  stored text is the text our model produced. Cost: extraction stops for documents that
  reach Papra by any *other* route (their intake email, the web UI), because our pipeline
  becomes the only extractor in the instance.
- **`custom-http`** is the option to choose if Papra-native intake has to keep producing
  content, at the price of an authenticated tailnet listener plus a bounded-window sweeper
  for the silent-permanent-failure case measured above.
- **Reconciliation alone** (keep both writers, re-patch) stays the weakest option: it
  accepts a writer that measurably lands ~2 s after upload and wins.
