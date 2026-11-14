# Papra Suite — subject-filter acceptance contract

Companion to `spec.md` and to
`docs/superpowers/specs/2026-11-12-papra-suite-implementation-decisions-design.md`.
Status: **proposal under review — not yet approved, not yet a plan input.** This
document is a prerequisite gate for the M4 dashboard: an operator must not be
given a text box that writes a regex into the poll loop until the rules below
decide what that box accepts.

Where this document disagrees with `spec.md` §5.6 or with the M3 residual-risk
wording in `.superpowers/m3-review-loop-state.md`, this document wins once
approved; each disagreement is flagged in §12.

Every number below is labelled either **measured** (reproducible on CPython 3.12
on the orchestrator's machine, prototype in §9) or **policy** (a choice). No
number is offered as proof of a property the prototype does not implement.

## 1. Scope, and an explicit change to the agreed gate

Operators type regular expressions into a subject filter. Those expressions run
against strings an outsider controls. M3 shipped the *runtime* half of this
(killable child, `papra_suite/safematch.py`), and its review left the save-time
half as "M4 blocker: save-time filter validation".

The gate was originally worded as a **conservative accepted subset**. This
design deliberately replaces that wording with a **layered heuristic**, because
the rules below do not bound matching *complexity*:

> Accepted patterns may still match in superlinear time. What the contract
> guarantees is **bounded work per attempt**, inside a bounded number of
> attempts per message (§2) — not bounded complexity.

The rejected alternative was a grammar with a provable complexity bound. It was
rejected as unverifiable without automata machinery we would be inventing, and
because it would have to reject `.*rechnung.*` — the most natural thing an
operator writes. This substitution is recorded here so that it is a decision on
the record rather than a gap discovered later.

## 2. The guarantee, stated to its limit

Three controls. They **overlap**: the deadline contains work that the structural
rules reject preventively and that the cap restricts preventively. Treating them
as disjoint coverage classes was an error in an earlier draft.

| cost class | example | measured | prevented by | contained by |
| --- | --- | --- | --- | --- |
| exponential backtracking | `(a+)+$` | 26 characters → 2.50 s | structural rules (§3) — the cap cannot help, subjects are longer than 26 chars | child deadline |
| combinatorial bounded repetition | `^(a?){100}a{100}$` | **>10 s at a 100-character subject** | structural rules — every size bound is satisfied, and the cap cannot help | child deadline |
| polynomial, sequential repeats | `^a*a*a*b$` | 1 000 chars → 0.074 s; 10 000 → >8 s | cap only | child deadline |
| polynomial, search restarts | `a+b`, `(a|b)+x` (unanchored) | 1 000 → 0.006 s; 16 000 → 1.39 s; 60 000 → >10 s | cap only | child deadline |
| decoder work before matching | 2 M raw chars of encoded words | **6.8 s to decode** | raw limit (§4) | **nothing** — see residual 7 |
| anything unforeseen | — | — | — | child deadline |

**Attempts per message — corrected, and this was wrong in an earlier draft.**
`_request` (`safematch.py:269-321`) retries on `EOFError`, `OSError`,
unparseable replies and write failures **without regard to how long the child
ran**. A child that consumes nearly the whole budget and then dies therefore
lands in a retried path, and a child killed by its own `SIGALRM` can present as
EOF rather than as the parent's `TimeoutError`. The honest bound:

- a **parent-detected** timeout raises `MatchTimeout` and is **not** retried
  (`safematch.py:271-273`);
- everything else retries until `MAX_CHILD_RETRIES = 2`, so the worst case is
  **`MAX_CHILD_RETRIES + 1 = 3` attempts**, i.e. up to ~3 × 2 s ≈ **6 s** of
  child CPU per message per mailbox;
- the parent's 2.0 s deadline and the child's self-deadline of
  `ceil(2.0) = 2` s **race**, and either can win, so which path a given message
  takes is not predictable.

Closing that gap is work, not prose: either a deadline shared across retries or
reliable classification of a child-alarm death as a timeout. Until one exists,
§11 records 6 s as the bound rather than 2 s.

**Aggregate bound — corrected.** One `KillableMatcher` is created per mailbox
poll task (`mail.py:485`), plus one in the pipeline (`pipeline.py:227`), so
concurrent regex children are bounded by **enabled mailboxes + 1**.
`OCR_CONCURRENCY` bounds OCR jobs and has **no** relationship to regex children;
citing it as an aggregate matcher bound was wrong.

Stated negatively, and this is load-bearing prose:

- `SUBJECT_FILTER_TIMEOUT_S` bounds **one attempt inside one child**. It is not
  a bound on poll latency, which also contains header decoding, IMAP
  round-trips, Papra uploads, retries, IPC and process cleanup.
- It is not a service-availability guarantee. Enabled mailboxes times 2 s of
  concurrent child CPU is a real load on a home server, and sustained hostile
  traffic can saturate it while every individual deadline is honoured.
- Neither the cap nor the structural rules establish that an accepted pattern is
  cheap: `(a|b)+x` is accepted and is quadratic, measured above.

## 3. Control 1 — structural acceptance

Three rules over the **parse tree**, never over source text, plus the analysis
bounds. Rules are stated so an operator can be told why their pattern was
refused in one sentence.

**R1 — inside a repetition of two or more (or an unbounded repetition), the
repeated body must have a fixed width and must not be able to match the empty
string.** This is the rule that catches the class an earlier draft of this
document accepted: `^(a?){1000}a{1000}$` and `^(a?){100}a{100}$` are 17–19
characters, two repetition nodes, depth 4, 2 repetitions — inside every size
bound — and measured **>10 s at a 100-character subject**, which is a normal
subject length, so the length cap does not contain them. A variable-width or
empty-matching body under repetition is what lets one input be split many ways.

**R2 — no alternation inside a repetition.** Catches what R1's width arithmetic
structurally cannot see, because CPython rewrites alternations: `(?:(a)|a)+x`,
`(a|aa)+b`. Appendix A.1 records which rule fired for which fixture, and without
R2 those fixtures are accepted.

**R3 — constructs the analyser does not reason about are refused.**
Backreferences (`(a)\1+`) are rejected: no rule here says anything about them. So
is any opcode the traversal does not recognise.

**A bounded repetition of exactly one (`?`) is exempt**, which is what keeps
`^Rechnung[- ]?(1|II)?$`, `^(Rechnung|Invoice)?$` and `(R|I)?N\d{6}` acceptable:
one repetition of an alternation cannot multiply.

Measured against the corpora in Appendix A: **21 of 21 rejected, 31 of 31
accepted**. The rejected set includes `(Rechnung|Invoice)\s*(\d+\s*)+$`, which
scored **1.0** on M3's growth-ratio probe and was therefore accepted by it.
These numbers come from the **prototype** in `/tmp/finalrule4.py`, which
implements R1–R3 and the four size bounds but **not** the fail-closed opcode
adapter, and the validator that ships must re-run them before they are quoted as
evidence about the product (§9).

**What the policy costs, measured rather than glossed.** Rejected and *not*
catastrophic: `(Re: |WG: )+`, `(yes|no)*!`, `^(foo|bar){2}$`, and
`^(?:\d{1,3}\.){3}\d{1,3}$` (variable-width body). Measured cost of the first
two at the cap: `(Re: |WG: )+x` on 1 000 characters is **0.009 s**, on 10 000 it
is 0.221 s — the same profile as `(a|b)+x`, which is **accepted** (0.006 s /
0.135 s), because CPython happens to normalise `(a|b)` into a character class
and not `(Re: |WG: )` into anything. So the boundary here is drawn by
normalisation, not by a cost difference, and an operator who reads "no
alternation in a repetition" as a claim about danger is being told something
that measurement does not support. It is a claim about what the analyser can
prove cheaply. R2 is kept anyway: it is two lines, fails closed, and `(a|b)+`
survives only because normalisation collapsed it — a trivially written variant
does not.

**Analysis bounds.** Protect the analyser from hostile patterns: pattern length
≤ 512 characters, ≤ 512 parse nodes, depth ≤ 24, ≤ 64 repetition nodes.
Measured over the accepted corpus: longest 32 characters, busiest 31 nodes,
depth ≤ 4, ≤ 4 repetitions; a hand-written busy invoice filter reached 97
characters, 69 nodes, depth 5, 10 repetitions. The limits sit several times
above those samples; that is a **policy** choice about what operators plausibly
write, not evidence about real mailboxes, and **no bound here limits expanded
work** — which is exactly why R1 exists.

**Fail closed, and distinguish two kinds of "no".** The traversal is
opcode-specific: anything unfamiliar — an unexpected opcode, an unrecognised
child shape, a parse error, an analysis that runs out of its own budget — is a
**rejection of that pattern**, never acceptance. `re._parser` is private and the
project declares `>=3.12`, so it sits behind an adapter that makes the shape
assumptions explicit and fails closed when they stop holding. Two traps were
hit while prototyping and both belong in pins (§9): repetition and group bodies
are `re._parser.SubPattern`, which is **not** a `list` subclass, and CPython
stores some nodes as **two-element lists** rather than tuples — a traversal that
checks `isinstance(x, tuple)` silently skips branch bodies and reports a
catastrophic pattern as safe.

A **pattern-level** rejection means the filter is not saved and the operator is
told which rule fired and how to restructure. An **infrastructure-level** failure
(child will not start, IPC broke) is `UNAVAILABLE` and keeps M3's disposition:
poll error, cursor frozen, failure counted. Those two must never share a code
path — the same confusion cost a round of the M3 review.

## 4. Control 2 — subject length: two limits with two justifications

`MAX_SUBJECT_CHARS = 1000` already exists and applies on both paths
(`papra_suite/mail.py:266`, `:519`). What changes is the **disposition**.

**4.1 The decoded cap is a correctness rule.** `re.search(pattern,
subject[:1000])` manufactures an end-of-string the operator's `$` did not
approve, and destroys the tail a `$` needed. Reproduced with
`^Rechnung.*\d{3}$`:

- 1050 characters, digits landing on character 1000 → truncated **matches**,
  full subject does not. Mail is ingested that the filter rejected.
- 1109 characters, digits beyond character 1000 → truncated **does not match**,
  full subject does. Mail the filter wanted is silently never ingested.

Neither appeared in M3 review because the cap predates it and its comment
records only the resource benefit. So: over the cap, do not truncate and do not
evaluate.

**Rule (T2).** When a mailbox **has** a subject filter configured and the decoded
subject exceeds 1000 code points:

- Return `SUBJECT_TOO_LONG` — a distinct outcome, **not** `NOMATCH`.
- Disposition follows `TIMEOUT`'s: warning, skip ingestion, mark `\Seen`,
  advance the cursor. Consequence stated plainly: **this retires the message
  from automatic processing; nothing retries it.** Deliberate operator recovery
  may still exist; the automatic path does not.
- One redacted WARNING carrying mailbox id, UID, measured length and the cap —
  never the subject text, which is untrusted content bound for the dashboard's
  log page.
- A per-mailbox counter (semantics in §8).
- **No filter configured → rule does not apply.** No filter means ingest.

Measured on the **decoded** value: what `_decode_header_value` returns after
`decode_header`/`make_header`, before any whitespace collapsing; the matching
path receives that same string with no truncation anywhere.

**4.2 A raw-header limit returns, honestly labelled as an ingestion
restriction.** An earlier draft justified a raw limit as *implying* decoded
length. That is false, and the arithmetic is worth recording because it is the
reason the two limits cannot be merged: `decode_header` **discards whitespace
between adjacent encoded words**, so a 73-character raw header — two 16-character
encoded words and **41** characters of filler — decodes to `ab`. Inflation is
unbounded; no raw threshold can prove anything about decoded length.

The raw limit is nevertheless required, for a different reason: decoding happens
in the poller, **before** the child deadline exists. Measured, `decode_header`
plus `make_header` over 2 000 000 raw characters of encoded words costs **6.8 s**
(200 000 raw → ~155 ms), against a message buffer already capped at
`MAX_MESSAGE_BYTES` = 25 MiB (`RFC822.SIZE` checked before the fetch). A plain
ASCII subject of 8 M characters costs 1.3 ms — that number is a
*plain-header* observation and must not be quoted as a cost bound for hostile
headers.

So: **a raw `Subject` header value longer than 32 768 characters is
`SUBJECT_TOO_LONG` without decoding**, and the document states plainly that this
**can reject a subject whose decoded value would fit** — it is a bound on decoder
work, and it is a second ingestion restriction, not an optimisation. Both limits
apply only when a subject filter is configured.

`SUBJECT_TOO_LONG` is **not** evidence that the subject failed the regex. It is a
processing-domain restriction and no dashboard badge may imply "did not match".

Both caps are fixed named constants, not configuration: 1000 decoded code points
(product policy; RFC 5322 sets no subject limit at all) and 32 768 raw
characters. Making either configurable would multiply the corpus and let an
operator widen the thing the control exists to narrow.

## 5. Control 3 — the child deadline (shipped, re-described)

`SUBJECT_FILTER_TIMEOUT_S = 2.0`, a request-owned `SIGALRM` self-deadline in the
child with no Python handler installed (the default disposition kills
mid-`re.search`, where nothing running in that interpreter could), plus a
parent-side kill and the M3 orphan bound. A killed match is `TIMEOUT` → decided
non-match, `\Seen` set, cursor advances, which is what stops poison mail being
re-fetched and re-killed every poll.

What it is **not**: not a poll-latency bound; not an aggregate bound; not one
attempt per message (§2, up to three); not sufficient — the structural rules and
the caps exist because the deadline alone means paying 2 s per hostile message
forever.

## 6. Probes refute, they never accept

The growth-ratio probes stay, with their role narrowed to one direction:

> A probe result may only ever produce a **rejection**. No probe result is ever
> evidence that a pattern is acceptable.

M3 measured why: `(Rechnung|Invoice)\s*(\d+\s*)+$` blows up only on input
containing the pattern author's own literal prefix, so generic probes score it
1.0 — perfectly linear — while it needs 26 characters to exceed the deadline.
Probe inputs derived from literals extracted out of the pattern itself are
permitted as further **rejection** evidence, and are still not acceptance
evidence for a pattern the structural rules reject.

## 7. No escape hatch

No "I understand the risk" checkbox, no env var that disables R1/R2/R3 or either
cap, no per-mailbox override. A single-operator system has the same person as
author, attacker and victim; an escape hatch converts a stated bound into a
preference, and the person who flips it is the one who pays the child CPU.

## 8. Outcome vocabulary, dispositions, and their bookkeeping

`MatchOutcome` keeps `MATCH`, `NOMATCH`, `TIMEOUT`, `UNAVAILABLE`. The child
protocol gains **one** operation, `analyze` (§10); **no new length outcome is
added to it**, because both length decisions are made in the poller before the
matcher is called.

| outcome | decided by | ingestion | `\Seen` | cursor | visible as |
| --- | --- | --- | --- | --- | --- |
| `MATCH` | filter | permitted | set | advances | normal path |
| `NOMATCH` | filter | no | set | advances | normal path |
| `TIMEOUT` | child deadline | no | set | advances | warning + `filter_timeouts` counter |
| `SUBJECT_TOO_LONG` | length cap | no | set | advances | warning + `subject_too_long` counter |
| `INVALID_PATTERN` at save time | R1/R2/R3/bounds | n/a | untouched | untouched | dashboard error naming the rule |
| `INVALID_PATTERN` at startup (hand-edited row) | R1/R2/R3/bounds | undecided | **not set** | **frozen** | poll error, `record_poll_failure`, 3-strike path |
| `UNAVAILABLE` | child cannot run | undecided | **not set** | **frozen** | poll error, `record_poll_failure`, 3-strike path |

`MATCH` **permits** ingestion; it does not cause it. Parsing, size limits,
dedup, the Papra upload and the OCR job can all still refuse the message, and
the `\Seen`/cursor semantics for those failures are M2/M3 behaviour that this
document does not change.

**Bookkeeping invariants.**

- `SUBJECT_TOO_LONG` and `TIMEOUT` are **dispositions, not faults**: neither may
  call `record_poll_failure`, so neither can raise `consecutive_failures` or
  drive a mailbox into the 3-strike disable. A hostile sender must not be able to
  switch a mailbox off by sending long subjects. The qualification matters: a
  failure to *persist* one of these dispositions, or an IMAP error while
  applying it, is still a poll failure and still counts.
- Neither creates a job nor writes a `dedup_hashes` row.
- **Counters are approximate, and must be labelled that way.** Today
  `mark_seen()` (IMAP) runs at `mail.py:675` and the cursor write at `:691`, in
  separate steps; a crash in between leaves the message `\Seen` with the cursor
  behind, and because `fetch_since()` does not filter on `\Seen` the message is
  re-fetched and re-evaluated. So a counter counts **retired evaluations**, is
  monotone, and **can over-count** after a crash, a replay, a deliberate cursor
  reset or a UIDVALIDITY change. Exact-once would require an identity key
  (mailbox, UIDVALIDITY, UID) and an idempotency table; that is explicitly out of
  scope (§13), and the M4 status page must not present the number as exact.
- Counters are written in the same transaction as the cursor advance, so the
  over-count window is the IMAP/database gap and not a second one.

**Recovery from a disabled mailbox.** `record_poll_failure` sets `enabled = 0` at
the third consecutive failure (`store.py:470-476`); `record_poll_success` resets
`consecutive_failures` and `last_error` but **never restores `enabled`**
(`store.py:455-459`). So a mailbox disabled by a bad filter stays disabled until
the operator re-enables it, and saving a corrected filter **does not clear
`last_error` or `consecutive_failures`** — those fields also describe IMAP,
authentication and matcher-infrastructure failures, and validating a regex proves
nothing about them. The next genuinely successful poll clears them. Saving does
not flip `enabled` either: that flag is operator-controlled.

**Parity, honestly scoped.** `subject_matches` returns a bool and cannot
represent `UNAVAILABLE` or `SUBJECT_TOO_LONG`, so a promise that it "mirrors
every row" would be false. Instead the *semantics* — case-insensitivity, the
empty-pattern and `None`-subject branches, both caps and their dispositions —
live in one shared helper both paths call, and the poller/child path returns an
outcome-bearing type. Pins assert the helper's decisions, including both §4.1
reproducers rewritten to expect `SUBJECT_TOO_LONG`.

## 9. Corpora, pins, and the release gate

- **Canonical fixture artefact.** Appendix A is the corpus of record: every
  fixture with its expected verdict and rule name, committed alongside the
  implementation as a test fixture file so the numbers in this document are
  reproducible rather than remembered. The prototypes used to produce them are
  labelled as prototypes.
- **Must-reject: 21 fixtures** (Appendix A.1).
- **Must-accept: 31 fixtures** (Appendix A.2), each with `accept` recorded
  explicitly. A **compatibility** corpus: its purpose is to make
  over-tightening fail loudly, and it is not a safety corpus.
- **Accepted-but-slow corpus:** `^a*a*a*b$`, `a+b`, `(a|b)+x`, `.*x.*`. Pinned
  **behaviourally**, never by timing: over-cap subjects never reach the matcher;
  exactly-at-cap subjects reach it unchanged; an expensive accepted pattern is
  evaluated in the child and killed there. A test asserting "~75 ms" would fail
  on a slower laptop while the guard is intact.
- **Invalid-input fixtures:** patterns CPython itself refuses, including `(?i)`
  anywhere but the start (measured: `re.error: global flags not at the start`),
  rejected with CPython's own message.
- **Mutation pins:** R1, R2, R3, each size bound, the decoded cap and the raw
  limit each get a pin that fails when the rule is weakened.
- **Shape pins**, three of them, because the representation has already bitten
  twice: (i) a repetition node's body is reachable and `SubPattern` is not a
  `list`; (ii) node detection accepts **both** tuples and two-element lists, so a
  CPython change to either cannot silently hide branch bodies; (iii) `(a|b)+x`
  produces **no** `BRANCH` node while `(a|aa)+b` **does**.
- **Interface pins** for §10: analysis-timeout is distinguishable from
  infrastructure-unavailable; a validator that cannot run never yields
  acceptance.
- **Per-interpreter gate.** The corpora run on **3.12 and 3.13**, the supported
  set for this contract even though `pyproject` declares `>=3.12`; no further
  interpreter may poll mail before they have been run on it.

## 10. Where it runs, and the validation boundary

One module owns the contract, and **all** pattern work — parsing, the traversal,
compilation, and any probe — is delegated to the killable child through a single
`analyze` operation, so a pathological pattern cannot hang validation the way
`(a+)+\d+$` once hung the pre-validation probe. Nothing in the dashboard process
parses an operator-supplied pattern.

The parent-side interface returns a typed result, never a bool:

| result | meaning | caller's action |
| --- | --- | --- |
| `VALID` | accepted by R1/R2/R3 and inside all bounds | save / poll |
| `REJECTED` with rule name | structural rule or bound fired | save refused, rule shown |
| `UNSUPPORTED` | backreference or unknown opcode | save refused |
| `ANALYSIS_TIMEOUT` | the analysis itself blew its budget | save refused; **not** `UNAVAILABLE` |
| `UNAVAILABLE` | child could not run | nothing is decided; never treated as acceptance |

Analysis compiles with the **same flags as matching** (`re.IGNORECASE`), so
validator and matcher cannot disagree about the expression they were both handed.

Three call sites, all through that interface:

1. **Dashboard save path** — the primary gate: nothing is persisted that the
   contract rejects.
2. **The child**, as the `analyze` op that performs the work above.
3. **Startup pre-validation** of rows already in the database (hand-edited, or
   written by an older version), behaving as `INVALID_PATTERN` for that mailbox.

Startup validation alone is insufficient, because the dashboard makes filters
mutable at runtime. So a pattern is only ever used for matching if it was
validated **in this process run against that exact pattern string**: the poller
compares the stored pattern's hash against the validated one and re-validates on
any difference, including edits made while polling is running.

Probes and the ratio gate stay where they are, re-described per §6.

## 11. Residual risk register (accepted, dated 2026-11-14)

1. Accepted patterns may be superlinear: `(a|b)+x` measured 0.006 s at the cap
   and 1.39 s at 16 000 characters. Inside the cap this is cheap; the cap is
   what makes it cheap.
2. Worst case per hostile message per mailbox is **~6 s of child CPU** (three
   attempts, §2), not 2 s. Closing it to one attempt is named work.
3. Sustained hostile traffic can burn `enabled mailboxes × deadline` of CPU;
   there is no aggregate admission control and §13 keeps it that way.
4. Poll latency is not bounded by 2 s or 6 s: stalled IMAP is separately known
   to exceed Docker's stop grace and is tracked as M4 work.
5. `re._parser` is private and its representation is already irregular (tuple
   nodes and list nodes coexist). Mitigation is fail-closed traversal plus three
   shape pins, not API stability.
6. R1/R2 reject patterns that are not dangerous — `(Re: |WG: )+` measured 0.009 s
   at the cap — and the accepted/rejected boundary in that corner is decided by
   CPython's normalisation rather than by cost. Operators lose a construct and
   gain a rule name.
7. **Header decoding runs before any deadline.** Bounded by the 32 768-character
   raw limit; measured 155 ms at 200 000 raw characters, so the residual is
   roughly 150 ms per mail with a maximal subject header, once per message,
   unkillable.
8. Mail with subjects over the decoded cap, or raw headers over the raw limit, is
   never auto-ingested **when a subject filter is configured**. Accepted
   ingestion limit; countable, but the count is approximate (§8).

## 12. Deviations

- **From the M3 gate wording** ("conservative accepted subset"): replaced by the
  layered heuristic of §1, guarantee restated as bounded work per attempt within
  a bounded attempt count.
- **From `spec.md` §5.6 / §5.7**: the spec describes validation as "reject
  patterns that show super-linear backtracking on sample input", i.e. probe
  based. §6 forbids probes as acceptance evidence: probes refuse, structure
  accepts, caps restrict input, the deadline contains the rest.
- **From shipped M3 behaviour**: `MAX_SUBJECT_CHARS` changes from truncation to
  `SUBJECT_TOO_LONG` (§4.1), on the evidence that truncation is wrong in both
  directions.
- **From the first draft of this document**: the raw-header limit returns with a
  different justification (decoder work, explicitly able to over-reject); the
  "one budget-consuming attempt per message" claim is withdrawn; `MATCH`, the
  counters and the OCR_CONCURRENCY citation are corrected; status changes from
  "agreed design" to proposal.
- **Supported runtimes**: tested on 3.12 and 3.13, narrower than `pyproject`'s
  `>=3.12`. The floor is not raised, but no other interpreter polls mail before
  the corpora pass on it.

## 13. Out of scope

**New** aggregate admission control, rate limiting, a token bucket over filter
evaluation, or a global matcher concurrency cap; exact-once retirement counting
(an identity key plus an idempotency table); a load-dependent second, shorter
deadline tier for oversized subjects; a complexity-guaranteed regex grammar;
branch-overlap or branch-language analysis, which is where "reject only when the
alternatives overlap" leads; making either cap configurable; a
shared-across-retries deadline (named in §2 as the fix for residual 2, but not
delivered here); `regex`-module timeouts; changing `\Seen`/cursor semantics for
`TIMEOUT`.

## Appendix A — corpora of record

### A.1 Must-reject (21)

| fixture | rule |
| --- | --- |
| `(a+)+$` | R1 |
| `(a|aa)+b` | R2 |
| `(a|aa)+$` | R2 |
| `^(\w+\s?)*$` | R1 |
| `(Rechnung|Invoice)\s*(\d+\s*)+$` | R1 |
| `^(Rechnung|Invoice)[ ]*(\d+)+$` | R1 |
| `(a+)+\d+$` | R1 |
| `(x+x+)+y` | R1 |
| `(\s*\w+\s*){1,10}rechnung` | R1 |
| `(a)\1+rechnung` | R1, R3 (backreference) |
| `^(a*){20}b$` | R1 |
| `(a?)*b` | R1 |
| `(?:(a)|a)+x` | R2 |
| `^(a?){1000}a{1000}$` | R1 |
| `^(a?){100}a{100}$` | R1 |
| `(a?){10}b` | R1 |
| `^(a|a?){50}b$` | R1, R2 |
| `(a{1,3}){40}x` | R1 |
| `^(a|ab){40}x` | R2 |
| `(Re: |WG: )+` | R2 — **documented cost**, measured 0.009 s at the cap |
| `(yes|no)*!` | R2 — **documented cost** |

**R2 is load-bearing, not decoration.** The attributions above are the
prototype's own output, and R1 alone misses every pure-alternation case:
CPython factors the common prefix out of `(a|aa)` into `a(?:|a)`, whose width
looks fixed to a width computation that sums a branch's alternatives. A correct
per-branch width analysis would recover some of them, and is deliberately not
written, because R2 rejects the whole class outright for two lines of code. A
pin should assert this division of labour — R1 alone does **not** reject
`(a|aa)+b` — so that nobody deletes R2 on the assumption that R1 subsumes it.

`^(?:\d{1,3}\.){3}\d{1,3}$`, `^\d{1,3}(\.\d{1,3}){3}$` and `^(foo|bar){2}$` are
also rejected (R1 / R2) and are recorded here as additional costs so a future
relaxation has to be deliberate.

### A.2 Must-accept (31)

`.*rechnung.*` · `Rechnung` · `^Rechnung[- ]?\d{3,6}$` · `\b(Rechnung|Invoice)\b`
· `^(?:Rechnung|Invoice|Re)\b.*` · `IN[0-9/\-]{4,12}` · `\d{4}-\d{4}` ·
`^\S+@\S+\.\S+$` · `^(Rechnung|Rechnungen)\s*#?\d*` · `(?i)rechnung` ·
`[^@]*@[^@]*` · `^\s*(Rechnung|Invoice)\s+\d+\s*$` · `^.*\bMahnung\b.*$` ·
`(?<!\w)#\d+` · `^Q-?\d{5}$` · `\b(19|20)\d{2}\b` · `^(?!Abmeldung).*Rechnung.*` ·
`invoice[-_ ]?\d+` · `^(R|I)N-\d{6}$` · `^Rechnung[- ]?(1|II)?$` ·
`^(Rechnung|Invoice)?$` · `^(?:Rechnung|Invoice)?\s*\d+$` · `(R|I)?N\d{6}` ·
`(a|b)*x` · `(a|b)+x` · `(a|aa){0,1}b` · `^\d{4}-\d{2}-\d{2}$` ·
`^IN-\d{6}( / \d{4})?$` · `(?i)^(re:|wg:)? ?rechnung\b` · `^\d{4}-\d{4}$` ·
`^Q-?\d{5}$`

None of these is a safety claim. `(a|b)+x` and `.*rechnung.*` are accepted and
are superlinear; they are cheap only because of the decoded cap.
