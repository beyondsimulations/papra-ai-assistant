# Papra Suite — subject-filter acceptance contract

Companion to `spec.md` and to
`docs/superpowers/specs/2026-11-12-papra-suite-implementation-decisions-design.md`.
Status: **approved 2026-11-14 — ready for implementation planning.** This
document is a prerequisite gate for the M4 dashboard: an operator must not be
given a text box that writes a regex into the poll loop until the rules below
decide what that box accepts.

Where this document disagrees with `spec.md` §5.6 or with the M3 residual-risk
wording in `.superpowers/m3-review-loop-state.md`, this document wins once
approved; each disagreement is flagged in §12.

**Reading the numbers.** Every figure is labelled **measured** (CPython 3.12,
orchestrator's machine, exact subject lengths stated) or **policy** (a choice).
Corpus agreement is never a safety proof: §9 states what the corpora do and do
not establish.

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
operator writes. The substitution is recorded here so it is a decision on the
record rather than a gap discovered later.

## 2. The guarantee, stated to its limit

Three controls. They **overlap** and they are not interchangeable: prevention
stops a pattern from ever being stored; restriction shrinks the input it can see;
containment limits what the engine can do anyway. No control inherits another's
guarantee.

| cost class | example | measured, exact subject length | prevented by | contained by |
| --- | --- | --- | --- | --- |
| exponential backtracking | `(a+)+$` | 26 characters → 2.50 s | R1 | child deadline |
| combinatorial bounded repetition | `^(a?){100}a{100}$` | **>10 s at 100 characters** | R1 | child deadline |
| catastrophic inside an assertion | `(?=^(a+)+$)` | **>5 s at 999 characters** | R1 **only if the traversal descends into assertions** (§3) | child deadline |
| adjacent overlapping repetitions | `^a*a*a*a*b$` | **>5 s at 1 000 characters — at the cap** | R4 | child deadline |
| polynomial, sequential repeats | `^a*a*a*b$` | 0.071 s at 1 000; >8 s at 10 000 | rejected by R4 at the 3-member point; cap | child deadline |
| polynomial, search restarts | `a+b`, `(a|b)+x` | 0.005 s at 1 000; 1.39 s at 16 000; >10 s at 60 000 | cap | child deadline |
| header decoding before matching | 32 768 raw chars, maximal encoded words | **≤9 ms** at the raw limit; 6.8 s at 2 M raw, which the limit forbids | raw limit (§4.2) | **nothing** — decoding is outside the child (residual 7) |

**Attempts per message.** `_request` (`safematch.py:269-321`) retries on
`EOFError`, `OSError`, unparseable replies and write failures **without regard to
how long the child ran**. A child that consumes nearly the whole budget and then
dies lands in a retried path, and a child killed by its own `SIGALRM` can present
as EOF rather than as the parent's `TimeoutError`. Therefore:

- a **parent-detected** timeout raises `MatchTimeout` and is **not** retried
  (`safematch.py:271-273`);
- everything else retries until `MAX_CHILD_RETRIES = 2`, so the worst case is
  **3 attempts**, ≈ **6 s** of child CPU per message per mailbox;
- the parent's 2.0 s deadline and the child's `ceil(2.0) = 2` s self-deadline
  **race**, so which path a message takes is not predictable.

Closing this is **in scope for the prerequisite plan** (§13): one deadline per
message-wide filter evaluation, propagated across retries. Until it exists, this
document describes 6 s as *current behaviour* and 2 s as the *target*; the two
must not be conflated, and no §11 entry may quote the target as today's bound.

**Aggregate bound.** One `KillableMatcher` per mailbox poll task
(`mail.py:485`) plus one in the pipeline (`pipeline.py:227`): concurrent regex
children ≤ **enabled mailboxes + 1**. `OCR_CONCURRENCY` bounds OCR jobs and has
no relationship to regex children.

Stated negatively, and this is load-bearing prose:

- `SUBJECT_FILTER_TIMEOUT_S` bounds **one attempt inside one child** — not poll
  latency, which also contains header decoding, IMAP round-trips, Papra uploads,
  retries, IPC and cleanup.
- Not a service-availability guarantee: enabled mailboxes × 2 s of child CPU is
  real load, and sustained hostile traffic can saturate a home server while every
  deadline is honoured.
- **The cap does not make accepted patterns cheap.** `^a*a*a*a*b$` exceeds 5 s
  *at the cap*, which is what forced R4; and even accepted survivors are only
  cheap at plausible subject lengths, not by proof.

## 3. Control 1 — structural acceptance

Four rules over the **parse tree**, never over source text, plus the analysis
bounds. Each is stated so an operator can be told why their pattern was refused
in one sentence.

**R1 — inside a repetition of two or more (or an unbounded repetition), the
repeated body must have a fixed width and must not be able to match the empty
string.** Catches nested quantifiers and the combinatorial class an earlier
draft of this document accepted: `^(a?){1000}a{1000}$` is 19 characters, 2
repetition nodes, depth 4 — inside every size bound — measured **>10 s at a
100-character subject**.

**R2 — no alternation inside a repetition.** R1 cannot see equal-width ambiguous
alternatives: in `(?:(a)|a)+b` the repeated body is fixed-width and
non-nullable, so only R2 rejects it. R1 and R2 are independent — `(a+)+b` has no
alternation, `(?:(a)|a)+b` has no width variation — and neither subsumes the
other.

**R3 — constructs the analyser does not reason about are refused.**
Backreferences, and any opcode the traversal does not recognise.

**R4 — within a run of consecutive items that can all match the empty string,
three or more unbounded repetitions are rejected.** This is the class neither R1
nor R2 can see: `^a*a*a*a*b$` repeats single literals, fixed-width, non-nullable,
no alternation, and the work is the *distribution* of one input across
overlapping repetitions. Measured at the 1 000-character cap:

| unbounded members in an empty-matching run | measured |
| --- | --- |
| 2 (`^\w*\w*$`) | ≤ 0.001 s — accepted |
| 3 (`^a*a*a*b$`, `\w*\w*\w*`) | 0.071 s / 0.198 s — **rejected** |
| 3 with an empty-matching bounded member between (`^a*a*#?a*$`) | 0.082 s — **rejected** (the bounded member does not break the run) |
| 4 | >5 s — rejected |

Rejecting at three rather than at the measured fatal point (four) is a **policy**
margin. It costs nothing realistic: two empty-matching repetitions are already a
degenerate way to write one, and the natural `\s*#?\d*` shape contains only two
unbounded members and stays accepted.

**A bounded repetition of exactly one (`?`) is exempt** from R1/R2, which is what
keeps `^Rechnung[- ]?(1|II)?$`, `^(Rechnung|Invoice)?$` and `(R|I)?N\d{6}`
acceptable: one repetition of an alternation cannot multiply.

### 3.1 The traversal is a prerequisite, not later hardening

The rules above only mean what the traversal says they mean. Two defects found
while prototyping would each have shipped an unsound validator:

- **Assertions were not descended into.** `(?=^(a+)+$)` was *accepted* by an
  earlier analyzer and measures **>5 s at 999 characters**. `ASSERT` and
  `ASSERT_NOT` carry a nested sub-pattern that a generic payload walk does not
  find. That single hole also hides backreferences and unknown opcodes from R3.
- **Branch widths were summed across alternatives** instead of within each
  branch, which made `(a|aa)+b` look fixed-width. An earlier revision of this
  document attributed that to CPython's prefix factoring and even proposed a pin
  to preserve it. That attribution was **wrong**: factoring leaves `(a|aa)`
  width 1–2, and the miss was this arithmetic defect. It is recorded here so the
  correction is not lost.

Therefore the shipping validator must: descend into assertion bodies, group
alternatives per branch, and **never silently drop a child it does not
understand** — an unrecognised shape is a rejection. A branch whose width cannot
be computed is **unknown**, and unknown must reject; it must never default to a
fixed width.

**Counting conventions.** Bounds are computed over the **whole forest** of
top-level nodes, summed, not over the largest top-level subtree — an earlier
prototype took the maximum and therefore undercounted repetitions spread across
top-level siblings. Depth is measured from the top-level node at 1; node count
includes every visited node; repetition count includes repetitions inside
assertions and classes of repetition.

**Analysis bounds** (policy, protecting the analyser): pattern length ≤ 512
characters, ≤ 512 nodes, depth ≤ 24, ≤ 64 repetition nodes. Measured over the
accepted corpus: longest 32 characters, busiest 31 nodes, depth ≤ 4, ≤ 4
repetitions; a hand-written busy invoice filter reached 97 characters, 69 nodes,
depth 5, 10 repetitions. **No bound here limits expanded work** — which is
exactly why R1 and R4 exist.

**Fail closed, and distinguish two kinds of "no".** Anything unfamiliar — an
unexpected opcode, an unrecognised child shape, a parse error, an analysis that
runs out of its own budget — is a **rejection of that pattern**, never
acceptance. `re._parser` is private and the project declares `>=3.12`, so it sits
behind an adapter that makes the shape assumptions explicit and fails closed when
they stop holding. Two representation traps are already known and both belong in
pins: repetition and group bodies are `re._parser.SubPattern`, which is **not** a
`list` subclass; and CPython stores some nodes as **two-element lists** rather
than tuples, so a traversal that checks `isinstance(x, tuple)` silently skips
branch bodies and reports catastrophic patterns as safe.

A **pattern-level** rejection means the filter is not saved and the operator is
told which rule fired and how to restructure. An **infrastructure-level** failure
is `UNAVAILABLE` and keeps M3's disposition: poll error, cursor frozen, failure
counted. Those two must never share a code path — that confusion cost a round of
the M3 review.

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

Neither appeared in M3 review because the cap predates it and its comment records
only the resource benefit. So: over the cap, do not truncate and do not evaluate.

**Rule (T2).** When a mailbox **has** a subject filter configured and the decoded
subject exceeds 1000 code points:

- Return `SUBJECT_TOO_LONG` — a distinct outcome, **not** `NOMATCH`.
- Disposition follows `TIMEOUT`'s: warning, skip ingestion, mark `\Seen`, advance
  the cursor. Consequence stated plainly: **this retires the message from
  automatic processing; nothing retries it.** Deliberate operator recovery may
  still exist; the automatic path does not.
- One redacted WARNING carrying mailbox id, UID, measured length and the cap —
  never the subject text, which is untrusted content bound for the dashboard's
  log page.
- A per-mailbox counter (§8).
- **No filter configured → rule does not apply.** No filter means ingest.

Measured on the **decoded** value: what `_decode_header_value` returns after
`decode_header`/`make_header`, before whitespace collapsing; the matching path
receives that same string, untruncated.

**4.2 The raw-header limit, labelled as the ingestion restriction it is.** An
earlier draft justified a raw limit as *implying* decoded length. False, and the
arithmetic is the reason the two limits cannot merge: `decode_header`
**discards whitespace between adjacent encoded words**, so a 73-character raw
header — two 16-character encoded words and **41** characters of filler — decodes
to `ab`. Inflation is unbounded; no raw threshold proves anything about decoded
length.

The raw limit is still required, for a different reason: decoding happens in the
poller, **before** any child deadline exists. Measured: `decode_header` plus
`make_header` over 2 000 000 raw characters of encoded words costs **6.8 s**, and
200 000 raw costs ~155 ms. At the 32 768-character limit itself the worst measured
case is **≤9 ms**. (A plain ASCII subject of 8 M characters costs 1.3 ms — a
*plain-header* observation, not a bound for hostile headers.)

So: **a raw `Subject` header value longer than 32 768 characters is
`SUBJECT_TOO_LONG` without decoding**, and this **can reject a subject whose
decoded value would fit**. It bounds decoder work; it is a second ingestion
restriction, not an optimisation. Both limits apply only when a subject filter is
configured.

**Header semantics, defined rather than inherited.** The poller filters the value
of `message.get("Subject")` (`mail.py:154`), which is the **first** `Subject`
header; a second `Subject` header is invisible to the filter and may be what
other software displays. That asymmetry is documented behaviour, not a security
property, and the dashboard must not imply it vetted "the" subject. Folded
headers are unfolded by the email parser before measurement. A header that fails
to decode is `INVALID`-adjacent and must be logged and skipped, never silently
treated as an empty subject.

`SUBJECT_TOO_LONG` is **not** evidence that the subject failed the regex. It is a
processing-domain restriction and no dashboard badge may imply "did not match".

Both caps are fixed named constants, not configuration: 1000 decoded code points
(policy; RFC 5322 sets no subject limit) and 32 768 raw characters.

## 5. Control 3 — the child deadline (shipped, re-described)

`SUBJECT_FILTER_TIMEOUT_S = 2.0`, a request-owned `SIGALRM` self-deadline in the
child with no Python handler installed (the default disposition kills
mid-`re.search`, where nothing running in that interpreter could), plus a
parent-side kill and the M3 orphan bound. A killed match is `TIMEOUT` → decided
non-match, `\Seen` set, cursor advances, which is what stops poison mail being
re-fetched and re-killed every poll.

What it is **not**: not a poll-latency bound; not an aggregate bound; not one
attempt per message (§2, currently up to three); not sufficient on its own — the
rules and caps exist because a deadline alone means paying 2 s per hostile
message forever.

## 6. Probes refute, they never accept

> A probe result may only ever produce a **rejection**. No probe result is ever
> evidence that a pattern is acceptable.

M3 measured why: `(Rechnung|Invoice)\s*(\d+\s*)+$` blows up only on input
containing the pattern author's own literal prefix, so generic probes score it
1.0 — perfectly linear — while it needs 26 characters to exceed the deadline.
Probe inputs derived from literals inside the pattern are permitted as further
**rejection** evidence, and are never acceptance evidence.

## 7. No escape hatch

No "I understand the risk" checkbox, no env var disabling R1–R4 or either cap, no
per-mailbox override. A single-operator system has the same person as author,
attacker and victim; an escape hatch turns a stated bound into a preference, and
the person who flips it is the one who pays the child CPU.

## 8. Outcome vocabulary, dispositions, and their bookkeeping

`MatchOutcome` keeps `MATCH`, `NOMATCH`, `TIMEOUT`, `UNAVAILABLE`. The child
protocol gains **one** operation, `analyze` (§10); **no length outcome is added**,
because both length decisions are made in the poller before the matcher is
called.

| outcome | decided by | ingestion | `\Seen` | cursor | visible as |
| --- | --- | --- | --- | --- | --- |
| `MATCH` | filter | permitted | set | advances | normal path |
| `NOMATCH` | filter | no | set | advances | normal path |
| `TIMEOUT` | child deadline | no | set | advances | warning + `filter_timeouts` |
| `SUBJECT_TOO_LONG` | length cap | no | set | advances | warning + `subject_too_long` |
| `INVALID_PATTERN` at save time | R1–R4/bounds | n/a | untouched | untouched | dashboard error naming the rule |
| `INVALID_PATTERN` at startup (hand-edited row) | R1–R4/bounds | undecided | **not set** | **frozen** | poll error, `record_poll_failure`, 3-strike |
| `UNAVAILABLE` | child cannot run | undecided | **not set** | **frozen** | poll error, `record_poll_failure`, 3-strike |

`MATCH` **permits** ingestion; it does not cause it. Parsing, size limits, dedup,
the Papra upload and the OCR job can still refuse the message, and the
`\Seen`/cursor semantics for those failures are M2/M3 behaviour unchanged here.

**One terminal disposition per message.** A message that goes through two respawns
and then times out must produce **one** disposition, **one** `\Seen` update, **one**
cursor advance and at most **one** increment of one counter. Retries, worker
replacement and malformed replies are mechanics; they are not outcomes. The plan
must state where that single decision is recorded, and pins must show counters do
not move per attempt.

**Bookkeeping invariants.**

- `SUBJECT_TOO_LONG` and `TIMEOUT` are **dispositions, not faults**: neither may
  call `record_poll_failure`, so neither raises `consecutive_failures` nor reaches
  the 3-strike disable — a hostile sender must not be able to switch a mailbox off
  by sending long subjects. Qualification: a failure to *persist* a disposition,
  or an IMAP error while applying one, **is** a poll failure and still counts.
- Neither creates a job nor writes a `dedup_hashes` row.
- **Counters are approximate and must be labelled that way.** `mark_seen()` (IMAP)
  runs at `mail.py:675` and the cursor write at `:691`, separately; a crash
  between them leaves the message `\Seen` with the cursor behind, and since
  `fetch_since()` does not filter on `\Seen`, the message is re-fetched and
  re-evaluated. So a counter counts **retired evaluations**, is monotone, and
  **can over-count** after a crash, replay, cursor reset or UIDVALIDITY change.
  Exact-once needs an identity key (mailbox, UIDVALIDITY, UID) plus an idempotency
  table; out of scope (§13), and the status page must not present it as exact.
- A counter increment and its cursor advance share one transaction, so the
  over-count window is the IMAP/database gap only.

**Recovery from a disabled mailbox.** `record_poll_failure` sets `enabled = 0` at
the third consecutive failure (`store.py:470-476`); `record_poll_success` resets
`consecutive_failures` and `last_error` but **never restores `enabled`**
(`store.py:455-459`). A mailbox disabled by a bad filter therefore stays disabled
until the operator re-enables it, and saving a corrected filter **does not clear
`last_error` or `consecutive_failures`** — those fields also describe IMAP,
authentication and infrastructure failures, and validating a regex proves nothing
about them. The next genuinely successful poll clears them. Saving does not flip
`enabled` either.

**Parity, honestly scoped.** `subject_matches` returns a bool and cannot represent
`UNAVAILABLE` or `SUBJECT_TOO_LONG`, so "mirrors every row" would be false. The
*semantics* — case-insensitivity, the empty-pattern and `None`-subject branches,
both caps and their dispositions — live in one shared helper both paths call, and
the poller/child path returns an outcome-bearing type. Pins assert the helper,
including both §4.1 reproducers rewritten to expect `SUBJECT_TOO_LONG`.

## 9. Corpora, pins, and the release gate

- **Canonical fixture artefact.** Appendix A is the corpus of record, committed as
  a test fixture with per-fixture verdict **and rule name**, so these numbers are
  reproducible rather than remembered.
- **Must-reject: 29 fixtures** (A.1). **Must-accept: 39 fixtures** (A.2).
  Measured with `/tmp/finalrule5.py`.
- **What these numbers establish, and what they do not.** They establish that
  this rule set, with that traversal, classifies these fixtures as intended. They
  do **not** establish safety, completeness of the traversal, or that no accepted
  pattern is expensive — `(?=^(a+)+$)` was accepted by the immediately preceding
  prototype, which is the standing argument against treating a green corpus as a
  guarantee.
- **Accepted-but-slow corpus:** `^a*a*a*b$`-class at the run-of-two boundary,
  `a+b`, `(a|b)+x`, `.*x.*`. Pinned **behaviourally**, never by timing: over-cap
  subjects never reach the matcher; exactly-at-cap subjects reach it unchanged;
  an expensive accepted pattern is evaluated in the child and killed there. A test
  asserting a millisecond figure would fail on a slower laptop with the guard
  intact.
- **Invalid-input fixtures:** patterns CPython itself refuses, including `(?i)`
  anywhere but the start (measured: `re.error: global flags not at the start`),
  rejected with CPython's own message.
- **Traversal pins** (new, and the highest-value pins in this document): a
  catastrophic repetition inside `(?=…)` and inside `(?<=…)` is rejected; an
  alternation inside an assertion is seen by R2; a branch's width is computed per
  branch; unknown opcodes and unknown operand shapes reject.
- **Counting pins:** node/depth/repetition totals summed over the whole forest;
  a pattern with repetitions split across top-level siblings must hit the bound.
- **Shape pins:** repetition/group bodies reachable and `SubPattern` is not a
  `list`; node detection accepts both tuples and two-element lists; `(a|b)+x`
  produces **no** `BRANCH` node while `(a|aa)+b` **does**.
- **Rule-necessity pins:** R1 alone does not reject `(?:(a)|a)+b`; R2 alone does
  not reject `(a+)+b`; R1+R2 alone do not reject `^a*a*a*a*b$`. Deleting any rule
  turns a test red.
- **Interface pins** (§10): `ANALYSIS_TIMEOUT` distinguishable from
  `UNAVAILABLE`; a validator that cannot run never yields acceptance; retry
  sequences produce one disposition (§8).
- **Per-interpreter gate:** the corpora run on **3.12 and 3.13**, the supported
  set here even though `pyproject` declares `>=3.12`; no further interpreter
  polls mail before they pass on it.

## 10. Where it runs, and the validation boundary

One module owns the contract, and **all** pattern work — parsing, traversal,
compilation, probes — is delegated to the killable child through a single
`analyze` operation, so a pathological pattern cannot hang validation the way
`(a+)+\d+$` once hung the pre-validation probe. Nothing in the dashboard process
parses an operator-supplied pattern.

The parent-side interface returns a typed result, never a bool:

| result | meaning | caller's action |
| --- | --- | --- |
| `VALID` | accepted by R1–R4 and inside all bounds | save / poll |
| `REJECTED` + rule name | structural rule or bound fired | save refused, rule shown |
| `UNSUPPORTED` | backreference or unknown opcode/shape | save refused |
| `ANALYSIS_TIMEOUT` | the analysis itself blew its budget | save refused; **not** `UNAVAILABLE` |
| `UNAVAILABLE` | child could not run | nothing decided; never treated as acceptance |

**`ANALYSIS_TIMEOUT` must not be inferred from a bare EOF.** The protocol needs
framed replies carrying the request id, plus the child's exit status, so
"exceeded its own deadline" and "died unexpectedly" are different observable
events; where they cannot be told apart, the result is `UNAVAILABLE`, which is
the conservative answer because it decides nothing.

Analysis compiles with the **same flags as matching** (`re.IGNORECASE`), so
validator and matcher cannot disagree about the expression they were handed.

**Validation identity.** A validation result is bound to the tuple
**(pattern string, flags, policy version)**. Not the pattern alone: a
policy-version bump must invalidate every cached result, and hash comparison on
the pattern string cannot express that.

Three call sites, all through that interface:

1. **Dashboard save path** — the primary gate: nothing is persisted that the
   contract rejects.
2. **The child**, as the `analyze` op performing the work above.
3. **Startup pre-validation** of rows already in the database (hand-edited, or
   written by an older version), behaving as `INVALID_PATTERN` for that mailbox.

Startup validation alone is insufficient, because the dashboard makes filters
mutable at runtime. A pattern is used for matching only if it was validated in
this process run against that exact **(pattern, flags, policy version)**: the
poller re-validates on any difference, including edits made while polling runs,
and the M4 reconciler must treat a policy-version change as invalidating
everything. **Both** entry points enforce the same admission boundary:
`mail.py`'s poll path and `pipeline.py`'s matcher — a filter validated in one and
executed by the other is the bug this section exists to prevent.

Probes and the ratio gate stay where they are, re-described per §6.

## 11. Residual risk register (accepted, dated 2026-11-14)

1. Accepted patterns may be superlinear: `(a|b)+x` measured 0.005 s at the cap and
   1.39 s at 16 000 characters; run-of-two patterns such as `^\w*\w*$` measured
   ≤1 ms at the cap. Cheap at plausible subject lengths is not the same as cheap
   by proof.
2. Worst case per hostile message per mailbox is **~6 s of child CPU** (three
   attempts) — current behaviour, not the target. Closing it to one deadline is
   in scope for the prerequisite plan (§13).
3. Sustained hostile traffic can burn `enabled mailboxes × deadline` of CPU. There
   is no aggregate admission control and §13 keeps it that way.
4. Poll latency is bounded by neither 2 s nor 6 s: stalled IMAP is separately known
   to exceed Docker's stop grace, tracked as M4 work.
5. `re._parser` is private and its representation is irregular (tuple and list
   nodes coexist; assertion payloads are nested). Mitigation is fail-closed
   traversal plus the traversal, counting and shape pins — not API stability.
6. R1/R2/R4 reject patterns that are not dangerous — `(Re: |WG: )+` measured
   0.001 s at 1 000 characters and 0.053 s at 10 000 — and in that corner the
   accepted/rejected boundary is decided by CPython's normalisation of `(a|b)`
   rather than by cost. Operators lose a construct and gain a rule name.
7. **Header decoding runs before any deadline**, inside the poller. Bounded by the
   raw limit at **≤9 ms** measured; unkillable, once per message.
8. Subjects over the decoded cap, raw headers over the raw limit, and any
   `Subject` header other than the first are not evaluated against the filter
   when one is configured. Accepted ingestion and visibility limits; countable,
   approximately (§8).
9. Structural acceptance is a heuristic. R1–R4 are syntactic; each has now
   missed a class once in the making of this document (nested, bounded-body,
   hidden-in-assertion, adjacent-overlapping). The deadline is what makes a miss
   survivable.

## 12. Deviations

- **From the M3 gate wording** ("conservative accepted subset"): replaced by the
  layered heuristic of §1, guarantee restated as bounded work per attempt within
  a bounded attempt count.
- **From `spec.md` §5.6 / §5.7**: the spec describes probe-based validation
  ("reject patterns that show super-linear backtracking on sample input"). §6
  forbids probes as acceptance evidence: probes refuse, structure accepts, caps
  restrict input, the deadline contains the rest.
- **From shipped M3 behaviour**: `MAX_SUBJECT_CHARS` changes from truncation to
  `SUBJECT_TOO_LONG` (§4.1), on evidence that truncation is wrong in both
  directions.
- **From earlier drafts of this document**, in one place so nothing is quietly
  edited away: the raw limit was deleted and then reinstated with a different
  justification; "one budget-consuming attempt per message" withdrawn; "the cap
  makes accepted patterns cheap" withdrawn; `(a|b)+x` is not linear; the
  "CPython factoring makes `(a|aa)` look fixed-width" attribution was my own
  branch-width defect; `(Re: |WG: )+` timings used 4 000- and 20 000-character
  subjects while labelled 1 000 and 10 000; `MATCH` now "permits" ingestion;
  `OCR_CONCURRENCY` was cited as an aggregate matcher bound and is not one; the
  decode residual is ≤9 ms at the limit, not ~150 ms; corpus counts are per
  **distinct** pattern; status changed from "agreed design" to proposal.
- **Supported runtimes**: tested on 3.12 and 3.13, narrower than `pyproject`'s
  `>=3.12`. The floor is not raised; no other interpreter polls mail before the
  corpora pass on it.

## 13. Scope of this contract

**In scope for the prerequisite plan:** R1–R4 with a fail-closed total traversal
(assertions, branches, classes, unknown shapes rejecting); the `analyze` op and
its typed results; framed replies that separate deadline death from crash; the
**one message-wide deadline propagated across retries** that turns residual 2's
6 s into 2 s; the corpora as committed fixtures with the pin set of §9; both
admission entry points.

**Out of scope:** new aggregate admission control, rate limiting, token buckets or
a global matcher concurrency cap; exact-once retirement counting; a
load-dependent second deadline tier for oversized subjects; a complexity-
guaranteed regex grammar; branch-overlap or branch-language analysis; making
either cap configurable; `regex`-module timeouts; changing `\Seen`/cursor
semantics for `TIMEOUT`.

## Appendix A — corpora of record

Produced by `/tmp/finalrule5.py` (R1–R4 + bounds, assertions descended, per-branch
widths, forest-summed counts). Rule key: **R1∅** empty-matching body, **R1w**
variable-width body, **R2** alternation in a repetition, **R3↩** backreference,
**R3?** unknown opcode, **R4** ≥3 unbounded repetitions in an empty-matching run.

### A.1 Must-reject (29 distinct patterns)

| fixture | rule |
| --- | --- |
| `(a+)+$` | R1w |
| `(a|aa)+b` | R1w, R2 |
| `(a|aa)+$` | R1w, R2 |
| `^(\w+\s?)*$` | R1w |
| `(Rechnung|Invoice)\s*(\d+\s*)+$` | R1w |
| `^(Rechnung|Invoice)[ ]*(\d+)+$` | R1w |
| `(a+)+\d+$` | R1w |
| `(x+x+)+y` | R1w |
| `(\s*\w+\s*){1,10}rechnung` | R1w |
| `(a)\1+rechnung` | R1∅, R3↩ |
| `^(a*){20}b$` | R1∅, R1w |
| `(a?)*b` | R1∅, R1w |
| `(?:(a)|a)+x` | R2 |
| `(?:(a)|a)+b` | R2 — the R2-necessity witness |
| `^(a?){1000}a{1000}$` | R1∅, R1w |
| `^(a?){100}a{100}$` | R1∅, R1w |
| `(a?){10}b` | R1∅, R1w |
| `^(a|a?){50}b$` | R1∅, R1w, R2 |
| `(a{1,3}){40}x` | R1w |
| `^(a|ab){40}x` | R1w, R2 |
| `(a*)*(b*)*$` | R1∅, R1w |
| `(?=^(a+)+$)` | R1w — **the traversal witness**: rejected only because assertion bodies are descended into |
| `^a*a*a*a*b$` | R4 — measured >5 s **at the cap** |
| `^a*a*a*a*a*a*a*a*a*a*a*a*b$` | R4 |
| `^a*a*#?a*$` | R4 — a bounded empty-matching member does not break the run |
| `(Re: |WG: )+` | R2 — **documented cost**, 0.001 s at 1 000 |
| `(yes|no)*!` | R2 — **documented cost** |
| `^(?:\d{1,3}\.){3}\d{1,3}$` | R1w — **documented cost** |
| `^(foo|bar){2}$` | R2 — **documented cost** |

### A.2 Must-accept (39 distinct patterns)

`.*rechnung.*` · `Rechnung` · `^Rechnung[- ]?\d{3,6}$` · `\b(Rechnung|Invoice)\b`
· `^(?:Rechnung|Invoice|Re)\b.*` · `IN[0-9/\-]{4,12}` · `\d{4}-\d{4}` ·
`^\S+@\S+\.\S+$` · `^(Rechnung|Rechnungen)\s*#?\d*` · `(?i)rechnung` ·
`[^@]*@[^@]*` · `^\s*(Rechnung|Invoice)\s+\d+\s*$` · `^.*\bMahnung\b.*$` ·
`(?<!\w)#\d+` · `^Q-?\d{5}$` · `\b(19|20)\d{2}\b` · `^(?!Abmeldung).*Rechnung.*` ·
`invoice[-_ ]?\d+` · `^(R|I)N-\d{6}$` · `^Rechnung[- ]?(1|II)?$` ·
`^(Rechnung|Invoice)?$` · `^(?:Rechnung|Invoice)?\s*\d+$` · `(R|I)?N\d{6}` ·
`(a|b)*x` · `(a|b)+x` · `(a|aa){0,1}b` · `^\d{4}-\d{2}-\d{2}$` ·
`^IN-\d{6}( / \d{4})?$` · `(?i)^(re:|wg:)? ?rechnung\b` · `^Rechnung.*\d*$` ·
`^\S+@\S+$` · `^\s*(?:Mahnung|Zahlung)\b.*` · `^FW:\s*\w` · `^A-\d{3}(\.\d)?$` ·
`^(?:ab|cd|ef)\d{4}$` · `^\w*\w*$` · `^a*b$` · `^Rechnung\s?Nr\.?\s?\d+$` ·
`^a*a*b$` — accepted only as an R4 boundary case at two unbounded members; note
the three-member form `^a*a*a*b$` is rejected

None of these is a safety claim. `(a|b)+x`, `(a|b)*x` and `.*rechnung.*` are
accepted and superlinear; `\w*\w*` and `a*a*` sit at the accepted edge of the
adjacent-repetition class. They are cheap at plausible subject lengths, and the
deadline is what makes a surprise survivable.
