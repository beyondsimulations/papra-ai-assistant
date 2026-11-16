# OCR content search intermittently unreliable — characterisation, not a fix

Date: 2026-11-16
Status: **open problem.** No fix approved. A candidate workaround is on hold until the
reproduction is characterised, because the strongest claim made while investigating this
failed to survive its own control experiment.

Replaces an earlier draft of this document titled "Renaming a document destroys its OCR
content", which asserted a cause and prescribed a fix. That title was wrong and the document
was deleted rather than edited, so its claims are not left to circulate.

## Why this matters

`spec.md` acceptance criterion 5: search finds a document by a phrase occurring only in
OCR-written content. The planned MCP server (`2026-11-16-mcp-search-server-design.md`) makes
that search the primary interface. If content search is intermittently unreliable, an operator
is handed an empty result set for an archive that contains the document, and an agent will
report "you have no such invoice" with total confidence. This is a trust failure, not a
performance one.

## Experiment log

Papra image `ghcr.io/papra-hq/papra@sha256:d1f596a35332ad04d8a69d88a5e19bc82acd47f2bc2fe687dd2145b1ef5a8b19`,
built 2026-10-22. `docker compose pull` on 2026-11-16 returned the same digest. All probes ran
2026-11-16 against the local integration stack. `update_document` omits arguments that are
`None` (`papra_suite/papra_client.py:72-75`), so a rename sends `{"name": ...}` and nothing
else.

| # | sequence | fixture | result |
|---|---|---|---|
| 1 | upload → `PATCH(name+content)` → search content phrase | fitz PDF | found. **6/6** as the existing `test_full_document_round_trip` |
| 2 | upload → `PATCH(content)` → search content phrase | invalid-PDF bytes fixture | matched in one run, **never matched** in another |
| 3 | upload → `PATCH(content)` → search every 3 s for 90 s | invalid-PDF bytes fixture | found at t+0 s, **not found from t+15 s onward** |
| 4 | upload → `PATCH(content)` → 1 s → `PATCH(name)` → search | fitz PDF | **0/5 found**, immediately and +10 s |
| 5 | same documents as #4, then `PATCH(name+content)` | fitz PDF | **5/5 found** |
| 6 | upload → `PATCH(content)` → `PATCH(name)` → `GET` | invalid-PDF bytes fixture | returned document **without** the previously written marker |
| 7 | **control arm:** two docs per pair, both `PATCH(content)`; rename one, leave one; content asserted present immediately before rename; search + `GET` at t+0 and t+30 s | fitz PDF | **renamed 3/3 found and content intact; un-renamed 3/3 found and content intact** |

Probes #2, #3 and #6 used `tests/integration/test_mail_ingestion.py:PDF_BYTES`, which PyMuPDF
cannot open (`FzErrorFormat: no objects found`) — a bytes blob, not a PDF. Probe #7, the only
one with a control arm, used valid PDFs and additionally asserted content presence between the
content write and the rename.

Two differences between #4 (which failed 5/5) and #7 (which passed) remain unexplained and are
the whole of the open question: #4's new name contained a **prefix of the search phrase**
(`Rechnung Nr. 42 – flurb-.pdf`, searched as `flurb-<8 hex>`), and #4 had no control arm, no
pre-rename content assertion, and no observation past +10 s.

## Established

- Papra's `searchQuery` matches document **content** on this digest: probe #1, 6/6, and probe
  #7, 6/6 with a rename in half the population.
- `list_documents` items contain **no `content` field** — verified twice. Search results cannot
  carry snippets without a per-hit fetch. This is a fact, not a flake.
- **Conditional updates do not exist, and their absence fails open**: no `ETag`/`Last-Modified`
  offered; `If-Match: "not-a-version"` → 200 applied; `If-Unmodified-Since: 1999` → 200
  applied; unknown body field → 400. More precisely: the tested headers were ignored and the
  tested body field rejected. This is measured, reproducible, and independent of everything
  above.
- Existing tests do not cover the ordering question: `test_full_document_round_trip` writes
  name and content in one `PATCH` and never renames afterwards.

## Not established

- **That renaming destroys anything.** Probe #7 is the only controlled experiment and it
  contradicts probes #4–#6.
- That every deployment or document is affected, or that the effect is permanent when it
  occurs.
- Any cause. Papra's search is SQLite FTS5 over `(name, content)`; migration 0013 dropped the
  database triggers that synced the index and moved synchronisation to asynchronous
  application events; `indexDocuments` is a bare `INSERT` without conflict handling; at
  repository HEAD (2026-10-31 — newer than the image our `latest` pull returned, which says
  nothing about other published images) `updateDocuments` uses `omitUndefined` for partial
  updates. That is context. A bare `INSERT` does not explain an existing row losing content,
  and a missing `content` field in `GET` is not an index effect at all.
- Whether probe #6's missing content means content was cleared, or means the read happened to
  race a background write. The probe recorded a membership check, not the returned value.

## Characterisation plan

A bounded experiment matrix, not more one-off probes. Every cell runs **n=5** with a paired
control that is never renamed, and reports searchability **and** stored content at t+0, t+30,
t+300. Bounded deadlines: a probe is inconclusive if the archive does not answer within 15 s,
and inconclusive results are counted separately from failures, never as "not found".

Variables, one flipped at a time against a shared control:

1. Rename after content write vs no rename (re-run of #7, larger n).
2. New name containing a prefix of the search phrase vs an unrelated name.
3. Search term shape: single token vs hyphenated multi-token phrase.
4. Valid fitz-generated PDF vs the invalid bytes fixture.
5. Delay between content write and rename: 0 s, 1 s, 10 s, 60 s.
6. One organisation vs several.
7. Observation only vs an unrelated read (`GET`) interleaved, to test whether reads perturb it.

Completion rule: the matrix is complete when either a variable reproduces failure in ≥3 of 5
paired trials — at which point a fix spec gets written against a known reproduction — or all
cells report 0/5 with controls intact, at which point probes #2–#6 are recorded as
artefacts of their own construction, the specific confound is named, and this document closes.

## Interim protections, justified without a reproduction

These are cheap, and each is defensible on its own merits rather than on the unproven defect:

- **Pin the Papra test image by digest** and date the pin. `:latest` gave no version signal:
  pulling it returned the same Oct-22 build, which is how "it passed on my machine" became
  unfalsifiable.
- **A pipeline-shaped integration test** — ingest → OCR worker → title worker → search a
  content-only phrase, with a paired control document and a bounded settling window — as a
  capability check with an assertion on stored content as well as search. Not as a regression
  test for a defect we have not characterised, and not with "this must fail if the workaround
  is removed": that would contradict a removal criterion by design.
- **Never present an empty search as proof of absence.** In the dashboard and in the MCP
  server, an empty result is reported alongside what was searched and which organisations were
  reachable. This costs nothing and is the only honest posture while search integrity is an
  open question.

## On the workaround

The candidate fix was: read the document immediately before renaming, and send its freshly
read content with the new name; fail the job if the read fails or the content is not a string.
Measured repair, probes #4→#5.

**On hold.** Writing a permanent read-modify-write into `titles.py`, with a documented race
that can overwrite an operator's edits in Papra's own UI, is not a price worth paying for a
failure that a controlled run did not reproduce. If the matrix identifies a real reproduction,
the workaround returns with a target, a removal criterion, and the concurrency limitation
written down. If it does not, nothing ships.

Either way the compatibility behaviour stays out of `PapraClient.update_document`, which must
keep sending exactly the arguments it is given.

## Upstream

If the matrix yields a reproduction, report it with: the pinned digest, the paired
reproduction, the repair by resending content, and the `If-Match` / `If-Unmodified-Since`
behaviour (accepted and ignored). Without a reproduction there is nothing to file, and a
report of "search sometimes misses" is noise.
