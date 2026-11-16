# OCR content search intermittently unreliable — characterisation, not a fix

Date: 2026-11-16
Status: **mechanism found and reproduced for the missing content writes** — see the addendum at
the foot of this file. A content write that lands before Papra's own asynchronous extraction
finishes is replaced by that extraction. Read the addendum before quoting anything above; the
sections below are kept in full because the sequence of claims and retractions is itself the
record. What the addendum does **not** explain, and does not claim to: the multi-word and
punctuation under-matching, and the cause of each individual old probe — those trials recorded no
timings, so their attribution stays inferred.

No fix is approved. The remediation needs its own short spec, because the fix the earlier
sections contemplated (read-then-write before a rename) targeted a mechanism that measured
clean.

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
8. **Query text that Papra's search grammar interprets.** Added after the matrix was written,
   because research into Papra's source turned up a search *language*: `packages/search-parser`
   parses `name:`, `content:`, `tag:`, `created:`, `date:`, `has:date`, `AND`/`OR`/`NOT` and
   quoting. Several of the original failing probes searched for text containing punctuation —
   `1.234,56`, hyphenated tokens, multi-word phrases — which such a parser may treat as field
   filters or operators rather than as text. If so, those probes measured the query parser and
   not the index, which would explain failures that no control run has been able to reproduce.
   Cells: bare single token, hyphenated token, decimal-with-comma, multi-word phrase, quoted
   phrase — same content in every cell, only the query shape varying.

Completion rule: the matrix is complete when either a variable reproduces failure in ≥3 of 5
paired trials — at which point a fix spec gets written against a known reproduction — or all
cells report 0/5 with controls intact, at which point probes #2–#6 are recorded as
artefacts of their own construction, the specific confound is named, and this document closes.

### Matrix result (2026-11-16)

All seven variables, 5 paired trials each, checked at t+0, t+30 and t+300 after the rename:
**zero misses, zero control misses, zero content loss.** Variables: baseline rename-vs-no-rename,
name containing a prefix of the phrase, hyphenated phrase, invalid-PDF bytes fixture, zero delay,
60 s delay, pairs spread across three organisations.

So the rename does not destroy anything, and probes #2–#6 are artefacts. The likely confound is
**query shape**, measured separately and reproducible on demand (see *Follow-up measurement*
below): multi-word, hyphenated and punctuation-bearing queries under-match on this image, which
describes most of the queries in the failed probes. Two caveats kept on the record rather than
smoothed over:

- The attribution is probable, not proven. `quokka-marmalade-protocol` is hyphenated and matched
  reliably in the existing integration test, while `zephy-1a` and `delta-1a` did not match in the
  follow-up probes. Token shape does not cleanly separate the two sets.
- The follow-up probes searched seconds after writing content; the matrix did too and found
  everything, using a fresh phrase per trial. A short-lived per-query cache remains an untested
  explanation for the difference.

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

## Follow-up measurement: query shape matters far more than the rename did

Later the same day, while checking Papra's source for a different reason, it turned out Papra
ships a search **query language** (`packages/search-parser`: `name:`, `content:`, `tag:`,
`date:`, `AND`/`OR`/`NOT`, quoting). Testing that against the pinned image produced behaviour
that is reproducible on demand, unlike everything above:

One organisation, two documents, content set the same way in both:

| content | `alpha` | `gamma` | `alpha gamma` |
|---|---|---|---|
| `alpha beta gamma delta` (plain.pdf) | found | found | found |
| `# alpha (beta) gamma; delta-1a` (punct.pdf) | found | **not found** | **not found** |

- `gamma` is present in both documents' content and is found in only one of them.
- `delta-1a`, a hyphenated token that is literally in the content, is not found — and neither is
  the `delta` half of it in that same document.
- Field syntax is **not** enforced on this image: `content:<token>` where the token appears only
  in a *filename* still matched, so the colon is not selecting a field.
- Multi-term queries behave like an intersection: `alpha gamma` returned only the document where
  both matched.

Why this matters more than the rename question: an agent's most natural query is two or three
words, and on this build that is exactly the shape that under-matches. `search_documents` would
answer "no such document" to a good question.

What this does **not** establish, and must not be quoted as established:

- It contradicts other runs. `quokka-marmalade-protocol` and `zephyrqx429` — both hyphenated or
  alphanumeric tokens in content — matched reliably in the existing test and in the matrix, while
  `zephy-1a` and `delta-1a` did not here. Nothing about token shape separates the two sets.
- The matrix (variables A–D, 5 paired trials each, three observation windows) showed **zero
  misses**, including for a document whose content contains punctuation. So punctuation in
  *content* is not sufficient to cause a miss; the miss appeared in a run where the same content
  shape was queried seconds after the write.
- Whether this is index staleness, a per-query result cache serving a value computed before the
  index caught up, or the query parser, is **unknown**. The Y-at-t+0-then-N pattern seen earlier
  is consistent with a cache; the matrix's zero misses are consistent with it too, because every
  trial there used a fresh phrase and therefore a fresh key.

### Third pass, done cleanly: what a single token actually does

Same day, after the write race was understood. Method changed in the one way that matters: every
document is read back with `GET` and its stored `content` verified **before** any search runs, and
no filename contains any query token, so a match can only be attributed to the token under test.
Scripts: `docs/superpowers/probes/2026-11-16-search-grammar/`. The pattern below is deterministic
across every trial.

| content | queried with | result |
|---|---|---|
| `alphabet` | `alpha` | **found** |
| `alpha` | `alphabet` | not found |
| `Rechnungen` | `Rechnung` | **found** |
| `Strassenverkehr` | `Strasse`, `Strassen` | **found** both |
| `Strasse` | `Strassen` | not found |
| `alpha` | `lpha` | not found |
| `AZ-99QK` | `99qk`, `AZ-99QK` | **found** both |
| `AZ-99QK` | `az99qk` (glued) | not found |
| `info@beispiel.de` | `info`, `beispiel`, `de` | **found** all three |
| `MwSt 19 Prozent` | `Mw`, `19` | **found** both |
| `Wäsche` | `Wäsche`, `Wä` | **found** both |
| `Wäsche` | `Waesche` | not found |
| `Straße` / `Grüße` | `Strasse` / `Grusse` | not found |
| `Café` / `Köln` | `Cafe` / `Koln` | **found** (so Papra's own folding is uneven — another reason not to imitate it) |

**Single tokens match by prefix, on the complete token, not as a substring.** `alpha` finds
`alphabet`; `Strassen` does not find `Strasse`; `lpha` finds nothing. Short tokens match, and with
prefix matching a short token is noise rather than silence — `Mw` finds `MwSt`, and `W` finds
`Wäsche`.

**Punctuation separates tokens; it does not destroy matching.** `AZ-99QK` is indexed as `az` and
`99qk`, and both halves find it. What does *not* work is gluing them: `az99qk` finds nothing.

Three corrections to what this document said earlier, all in the same direction — the grammar was
less broken than it looked:

1. "Hyphenated tokens under-match reproducibly" is **wrong as a statement about Papra**.
   `AZ-99QK` and `delta-1a`-shaped content are found by either half and by the whole string. A
   query that failed to find them was querying a string the index never contained, or was reading
   content that had already been replaced.
2. "`gamma` is present in both documents' content and found in only one" is **not explained by the
   grammar** and stays the one open anomaly here. The old run inferred the index state from search
   results alone and never read the document back; with the write race now known, the leading
   candidate is that the content at query time was not the content that was written. Re-running
   that exact pair with the `GET`-before-search method closes it, and nothing should be claimed
   about it until then.
3. The "index staleness / per-query cache" hypothesis is no longer needed for the single-token
   cases: with the two confounds removed, matching is deterministic across every trial. The cache
   hypothesis is not disproven for multi-word queries, which were never the shape we forward.

What follows for the MCP tokeniser is now concrete instead of defensive: split on anything that is
not a Unicode letter or digit, keep diacritics exactly as typed, never glue fragments back together,
never forward a phrase, and intersect the per-token id sets locally. Prefix matching means a hit is
a *prefix* hit and must be described that way to the caller: `Miete` legitimately returns
`Mietschulden`, and pretending otherwise would be the misleading part.

### Consequences for the MCP design

1. `search_documents` cannot present an empty result as "the archive does not contain this".
   Results must carry the query as sent and the orgs reached, and the tool description must say
   under-matching is known behaviour.
2. A query normalisation decision is now required and is not made yet (see MCP spec, open
   items): pass text through, quote it, or accept structured parameters.
3. For an upstream report, the minimal repro is the two-document table above: identical
   `PATCH(content)` path, both documents containing `gamma`, one found.


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

---

# Addendum, same day: the writer was found. It is Papra's own content extraction

Status: **measured, with a positive control and paired arms.** Everything below was run on
the pinned digest `sha256:d1f596a3…` against the scratch stack. Scripts and their raw logs are
committed under `docs/superpowers/probes/2026-11-16-content-extraction/` (numbered, logs suffixed
`.log`; read that directory's README first — it names the two known flaws in the scripts, one of
which changes how `06.log` must be read).

## The finding

**Papra extracts text from every uploaded document asynchronously and writes it into the
same `content` column our OCR worker writes.** On this pinned image the extraction is on by
default and cannot be switched off: `CONTENT_EXTRACTION_STRATEGY` is validated against
`internal | mistral-ocr | docling | azure-di | custom-http`, and setting it to `""` is a
fatal startup error (measured: the container exits with `CONFIG_VALIDATION_ERROR`). Quote that
narrowly: it proves *this image, left at its defaults, always has a second writer*, not that
every conceivable deployment does — `mistral-ocr`, `docling`, `azure-di` and `custom-http` are
other services with their own timings and failure modes. What holds for any of them is that our
write and the extractor's write land in one column, and whichever finishes last owns the
document. When a `PATCH(content)` lands before the extraction finishes, the extraction lands
afterwards and replaces what we wrote. This is a **write-integrity** problem, not a search
defect: the index faithfully follows whichever value is in the column.

**Positive control** (`03-positive-control.py`, n=1). Upload a `text/plain` document whose
own body is `THEIR-EXTRACTED-TEXT-9f3c\nsecond line\n`, then `PATCH(content: "OURS-PATCHED-VALUE-1")`
after the upload returns 200. At t+0.2 s the stored content is `THEIR-EXTRACTED-TEXT-9f3c…`,
and it is still that at t+20 s. The value that displaces ours is **the extraction's output**,
not a blank and not an index artefact. A second document, never patched, holds its own
extracted text from t+0.2 s — so the extraction is what writes it, always, patched or not.

**Paired arms** (`02-paired-arms.py`, n=5 each; the search token appears only in the content,
never in the filename — the earlier probes' filename tokens made every content search a name
match). Arm A, `PATCH(content)` at upload+0.00 s: content length 24 at t+0.2 s, then **0 from
t+2 s through t+60 s, and content-search MISS at every one of those ticks, 5/5**. Arm B,
`PATCH(content)` at upload+10.0 s: **content length 24 and search HIT at every tick through
t+60 s, 5/5.** The arms differ in one variable: when the write happened relative to upload.

**Delay sweep** (`07-delay-sweep.py`, n=3 each): writes at upload+1/3/5/8/15 s survived to
t+40 s, 15/15, for a 193-byte PDF on an idle server. So the window for that fixture is
usually well under a second — which is why a probe that PATCHes, then does anything else, can
see the content and never see it disappear.

## How long the window is, and why no sleep fixes it

Extraction latency measured on this digest (`04-`, `05-`, `06-` probes):

| fixture | extraction visible by |
|---|---|
| 2-page fitz PDF with a text layer | t+0.24 s (non-empty and unchanged) |
| 1-page 200-dpi image-only scan | t+1.0 s (Tesseract: `GESCANTER TEXT 4211`) |
| 8-page 200-dpi scan, 17.7 MiB | t+8.2 s (5,639 characters) |

And under load, the window outlives any fixed wait. `06-scan-race-and-repair.py`, 8-page
scans, n=4: two documents had already lost the write by t+20 s; the other two still held it at
t+20 s, were **re-`PATCH`ed at t+20 s**, and by t+65 s both held 5,639 characters — Papra's
extraction, over a write that had already survived twenty seconds. Read the lengths, not the
survived flag: that probe's last-line flag was computed against a literal `'token'` and is
wrong for every row (the two 18-character rows *are* the probe's own content, and both
searched as HIT). So: **a rewrite is not durable, and "wait N seconds then write" is not a
synchronisation contract.** There is no field on the document that reports extraction state
(`GET` returns `content` only; no `contentExtraction*`, no `ETag`, no status).

## What this explains, and what it does not

Explains, for the record: a content write near upload time is destroyed, so "matched at t+0,
stopped matching later" (probes #2, #3), "`GET` returned the document without the marker"
(probe #6), and a burst of same-shaped trials failing together (probe #4, 0/5) are all
consistent with this writer. The matrix's 35 clean trials are also consistent with it: on an
idle server the window for a small fixture is sub-second, and every matrix trial wrote content
and then did other HTTP work before observing it. **"Consistent with" is the strongest phrase
available here** — none of those trials recorded when it wrote relative to upload, so their
cause stays inferred even though the mechanism itself is measured.

Does **not** establish, and must not be quoted as establishing:

- **That renaming destroys anything.** Still withdrawn. A rename-only `PATCH` measured after
  settle left the content intact at t+1, t+10 and t+30 (`04-window-and-burst.py`, arm C, n=1).
  The rename was never the writer; it was adjacent in time to the upload.
- That this was the cause of the specific failures in probes #2–#6. Those trials recorded no
  timings, so their attribution stays probable. What is now proven is that a different,
  reproducible mechanism exists and is always on.
- That the multi-word / punctuation under-matching in *Follow-up measurement* above is
  explained by this. It is a separate observation, and its own control (a term absent from one
  document but present in the other) still needs re-running: `gamma` "missing" from a document
  whose content had already been replaced by the extraction is this bug, not query grammar,
  and the probes cannot currently tell those two apart.

## What this does to the product, in one paragraph

Two writers, one column, no completion signal. Our OCR worker normally lands well after the
extraction — real OCR takes seconds — so today the pipeline usually wins by accident, and the
sweep's seen-table then records the document as processed so nothing re-writes it. When we
lose the race the outcome is silent: the document carries Papra's Tesseract text instead of
our markdown, the job is `done`, and the operator has no signal. Two further consequences,
both for the operator to rule on rather than for this document to assume: the extraction works
on scans on this image without any external model, which asks whether our OCR worker duplicates
a Papra feature that is always on; and `custom-http` is a supported strategy, which means Papra
can be configured to *ask us* for content instead of racing us for the column — writer
ownership rather than reconciliation.

## Consequences for the plans written from this document

1. An acceptance test that asserts searchable content at t+0 proves nothing about durability.
   The MCP plan's criterion-5 test asserts after an observable boundary (content stable across
   two reads) and reports the window it waited through, labelled as evidence and not as a
   guarantee.
2. Remediation is **not** in either current plan. It gets its own short correctness spec, with
   a target, a removal criterion, and the conflict policy written down — the same bar this
   document set for the rename workaround. Options on the table: own the column by serving
   Papra's `custom-http` extraction; reconcile with a bounded observation window and a visible
   failure state; or accept the loss and make it observable through `ingest_events`.
3. The remediation is not the withdrawn workaround, and does not reinstate it: the
   read-then-`PATCH(name+content)` before a rename targeted a mechanism that measured clean.

