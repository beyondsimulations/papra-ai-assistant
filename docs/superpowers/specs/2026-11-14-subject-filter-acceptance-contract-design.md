# Papra Suite — subject-filter acceptance contract

Companion to `spec.md` and to
`docs/superpowers/specs/2026-11-12-papra-suite-implementation-decisions-design.md`.
Status: **agreed design, awaiting implementation plan.** This document is a
prerequisite gate for the M4 dashboard: an operator must not be given a text
box that writes a regex into the poll loop until the rules below decide what
that box accepts.

Where this document disagrees with `spec.md` §5.6 or with the M3
residual-risk wording in `.superpowers/m3-review-loop-state.md`, this document
wins; each disagreement is flagged in §12.

## 1. Scope, and an explicit change to the agreed gate

Operators type regular expressions into a subject filter. Those expressions run
against strings an outsider controls. M3 shipped the *runtime* half of this
(killable child, `papra_suite/safematch.py`), and its review left the
save-time half as "M4 blocker: save-time filter validation".

The gate was originally worded as a **conservative accepted subset**. This
design deliberately replaces that wording with a **layered heuristic**, because
the rules below do not bound matching *complexity*:

> Accepted patterns may still match in superlinear time. What the contract
> guarantees is **bounded work per message**, not bounded complexity.

The rejected alternative was a grammar with a provable complexity bound. It was
rejected as unverifiable without automata machinery we would be inventing, and
because it would have to reject `.*rechnung.*` — the most natural thing an
operator writes. This substitution is recorded here so that it is a decision on
the record rather than a gap discovered later.

## 2. The guarantee, stated to its limit

Three controls, measured on CPython 3.12. Each covers a class the others do
not; none covers the others' classes.

| class | example | measured | covered by |
| --- | --- | --- | --- |
| exponential backtracking | `(a+)+$` | **26 characters** → 2.50 s | the structural rules only — the length cap cannot help, a subject is longer than 26 chars |
| polynomial, sequential repeats | `^a*a*a*b$` | 1 000 chars → 0.074 s; 10 000 → >8 s | length cap only — no structural rule rejects it |
| polynomial, search restarts | `a+b` (unanchored) | 60 000 → 0.77 s; 250 000 → >8 s | length cap only |
| anything unforeseen | — | — | child deadline, §5 |

Stated positively: **each matching attempt is bounded by the parent's reply
budget and by the child's own self-deadline, and a match cannot outlive its
child.** The per-message accounting, read out of `safematch.py` rather than
assumed:

- A `TIMEOUT` ends the message's filter evaluation. It is deliberately **not
  retried** (`safematch.py:271-273`): retrying a catastrophic pattern buys
  nothing but a second burned budget.
- The respawn loop (`MAX_CHILD_RETRIES = 2`) is reached only on *fast* failures
  — child died between check and write, reply unparseable — because the
  budget-exceeded path raises immediately instead of looping. So at most one
  attempt per message consumes a full budget.
- The child's self-deadline is rounded up to whole seconds and can therefore
  exceed the parent's patience by less than a second. The parent's kill is the
  authoritative bound; the self-deadline is for the case where there is no
  parent left to do the killing.

Stated negatively, and this is load-bearing prose:

- `SUBJECT_FILTER_TIMEOUT_S` bounds **one match attempt inside one child**. It
  is not a bound on poll latency, which also contains IMAP round-trips, Papra
  uploads, retries, IPC and process cleanup.
- It is not a service-availability guarantee, and the controls below are not one
  either. N children at 2 s each are N busy cores; sustained hostile traffic can
  saturate a home server while every individual deadline is honoured. The
  concurrency limits that already exist (`OCR_CONCURRENCY` and the poll
  structure) stay operative and are the only aggregate bound there is; see §13.
- A length cap of 1000 and a structural rule set neither establish that an
  accepted pattern is cheap. More sequential repetitions exhaust the budget on
  short inputs. The deadline stays necessary because of what the other two do
  not prove.

## 3. Control 1 — structural acceptance

Two rejection rules and one refusal rule, evaluated over the **parse tree** and
never over source text — because CPython rewrites the expression before the
engine sees it. Measured directly: `(a|b)+x` contains **no `BRANCH` node at
all** (the single-character alternation is factored into a character class, and
the result really is linear), while `(a|aa)+b` **keeps** its `BRANCH` (unequal
branches cannot be factored) and really is catastrophic. A source-level rule
would treat the two identically and be wrong in both directions.

**R1 — an unbounded repetition whose body contains a repetition or an
alternation is rejected.** `*`, `+` and `{m,}` over an ambiguous body.

**R2 — a bounded repetition of two or more whose body contains an unbounded
repetition is rejected.** This is what catches `(\s*\w+\s*){1,10}rechnung` and
`^(a*){20}b$`: a finite outer count multiplied by an ambiguous interior is
still a combinatorial distribution of the same input.

**A bounded repetition over a fully bounded body is accepted**, and that
distinction is the whole reason the rules are split this way. A first draft said
"a repetition whose body contains an alternation" and was measured rejecting
`^(Rechnung|Invoice)?$`, `^Rechnung[- ]?(1|II)?$` and
`^(?:Rechnung|Invoice)?\s*\d+$` — ordinary optional prefixes whose cost is a
constant factor, because `?` can only repeat once. The unbounded/bounded split
separates the shapes that multiply ambiguity from the shapes that merely
multiply work by a constant.

**R3 — constructs the analyser does not reason about are refused.** Backreferences (`(a)\1+`) are rejected outright: no rule here says anything about them, and R1's reasoning does not cover them. So is any opcode the traversal does not recognise.

Measured against the corpus in §9: **14 of 14 catastrophic and unsupported
patterns rejected** — including `(Rechnung|Invoice)\s*(\d+\s*)+$`, which scored
**1.0** on M3's growth-ratio probe and was therefore accepted by it — and **28
of 28 realistic operator patterns accepted**, zero false rejections. The
accepted patterns include the harmless bounded alternations above and both
normalised `(a|b)*x` / `(a|b)+x`.

**The known cost of the policy is named, not hidden.** `(Re: |WG: )+` and
`(yes|no)*!` are rejected and are **not** catastrophic: their branches have
distinct first characters and fixed lengths. The rules cannot see that, because
seeing it means comparing branch languages, and the sharper test
(reject-only-when-branches-overlap) was rejected as the beginning of an
automata checker we could not verify. The operator gets a message naming the rule
and a restructuring hint; the rejected pattern is a bug report away from being
re-examined. The margin numbers in the bounds table below are **policy choices**
about what an operator plausibly writes, not evidence about real mailboxes.

**Analysis bounds.** A hostile pattern can also be hostile to the analyser, so
the contract additionally rejects patterns above four bounds. The busiest
filter in the realistic corpus (an alternation of invoice words, optional
prefixes, two numeric groups and a trailing year) measures 97 characters, 69
parse nodes, depth 5 and 10 repetitions, so the bounds sit roughly 5x above
anything a real mailbox needs:

| bound | limit | margin over busiest measured filter |
| --- | --- | --- |
| pattern length | 512 characters | 5.3x |
| parse nodes | 512 | 7.4x |
| tree depth | 24 | 4.8x |
| repetition nodes | 64 | 6.4x |

The subject is bounded by §4, and the frame handed to the child is bounded by
the pattern-length bound plus the subject cap — **not** by `MAX_MESSAGE_BYTES`,
which bounds the fetched message and says nothing about IPC. These bounds
protect the analyser; they are not what bounds matching cost.

**Fail closed, and distinguish two kinds of "no".** The traversal is
opcode-specific: it recognises the nodes it reasons about and treats anything
unfamiliar — an unexpected opcode, an unrecognised child shape, a parse error,
an analysis that runs out of its own budget — as a **rejection of that
pattern**, never as acceptance. `re._parser` is a private module and the
project declares `>=3.12` rather than one interpreter, so it sits behind a thin
adapter whose only job is to make the shape assumptions explicit and to fail
closed when they stop holding.

A **pattern-level** rejection (invalid, or refused by R1/R2/R3/bounds) means:
the filter is not saved, the operator is told which rule fired and how to
restructure. An **infrastructure-level** failure (child will not start, IPC
broke) is `UNAVAILABLE` and keeps M3's disposition: poll error, cursor frozen,
failure counted. Those two must never share a code path — the same confusion
cost a round of the M3 review.

## 4. Control 2 — subject length bound

`MAX_SUBJECT_CHARS = 1000` already exists and already applies on both paths
(`papra_suite/mail.py:266`, `:519`). What changes is the **disposition**: over
the cap, we stop pretending to have evaluated the subject.

**The shipped truncation is wrong in both directions.** `re.search(pattern,
subject[:1000])` manufactures an end-of-string that the operator's `$` did not
approve, and destroys the tail a `$` needed. Reproduced with
`^Rechnung.*\d{3}$`, a plausible operator filter:

- subject 1050 chars, digits landing on char 1000 → truncated **matches**, full
  subject does not. Mail is ingested that the filter rejected.
- subject 1109 chars, digits beyond char 1000 → truncated **does not match**,
  full subject does. Mail the filter wanted is silently never ingested.

Neither appeared in M3 review because the cap predates it and its comment
records only the resource benefit. Truncation is therefore rejected outright,
not merely superseded: it is not the safe direction, it fails both ways.

**Rule (T2).** When a mailbox **has** a subject filter configured and the
subject exceeds the cap:

- Do not truncate. Do not evaluate.
- Return `SUBJECT_TOO_LONG` — a distinct outcome, **not** `NOMATCH`.
- Disposition follows `TIMEOUT`'s: warning, skip ingestion, mark `\Seen`,
  advance the cursor. Documented consequence: **this retires the message from
  automatic processing. Nothing retries it.**
- One redacted WARNING carrying mailbox id, UID, measured length and the cap —
  never the subject text, which is untrusted content and would land in the
  dashboard's log page.
- A per-mailbox counter, monotone and restart-surviving, shown on the M4
  status page. It counts **retired messages, not evaluation attempts**, and is
  incremented **in the same transaction as the cursor advance**, so a crash
  between the two cannot double-count. Storage is M4's choice; the requirement
  is that skipping is countable rather than log-scrapable.
- **No filter configured → rule does not apply.** No filter means ingest;
  there is nothing to refuse.

`SUBJECT_TOO_LONG` is **not** evidence that the subject failed the regex. It is
a processing-domain restriction, and no dashboard badge may imply "did not
match".

**Measurement point.** The cap is measured on the **decoded** value, in Unicode
code points: what `_decode_header_value` returns after
`decode_header`/`make_header`, before any whitespace collapsing — and the
matching path receives that same string with no truncation anywhere.

There is deliberately **no raw-header shortcut**. A first draft of this section
refused any raw `Subject` longer than 32 768 characters without decoding it,
justified by a worst-case inflation of 27 raw characters per decoded code point.
That justification is false: `decode_header` **discards whitespace between
adjacent encoded words**, so a 73-character raw header of two encoded words and
65 filler characters decodes to `ab`. Inflation is unbounded, which means a raw
limit can reject a subject whose decoded value fits the cap — a second silent
ingestion restriction wearing an optimisation's clothes. Decoding is also not
the cost the shortcut existed to prevent: `decode_header` plus `make_header`
over an **8-million-character** subject measured **1.3 ms**, and the whole
message is already bounded by `MAX_MESSAGE_BYTES` (25 MiB) and
`MAX_POLL_BYTES` (100 MiB), checked on `RFC822.SIZE` before the fetch. The
decoded length is the only length this contract recognises.

The cap is one fixed named constant, 1000 decoded code points. It is a product
policy choice, not an RFC-derived safety threshold — RFC 5322 sets no subject
limit at all. Making it configurable would multiply the corpus and let the
operator widen the very thing the control exists to narrow. Any future
header-level restriction inherits this section's precondition: **only when a
subject filter is configured.**

## 5. Control 3 — the child deadline (shipped, re-described)

`SUBJECT_FILTER_TIMEOUT_S = 2.0`, enforced by a request-owned `SIGALRM`
self-deadline in the child with no Python handler installed (the default
disposition kills mid-`re.search`, where nothing running in that interpreter
could), plus a parent-side kill and the M3 orphan bound. A killed match is
`TIMEOUT` → decided non-match, `\Seen` set, cursor advances, which is what
stops poison mail from being re-fetched and re-killed every poll.

This section exists to record what it is **not**: not a poll-latency bound, not
a capacity bound, and not the reason the other two controls are unnecessary —
it is the control that covers the two controls' blind spots.

## 6. Probes refute, they never accept

The growth-ratio probes stay, with their role narrowed to one direction:

> A probe result may only ever produce a **rejection**. No probe result is ever
> evidence that a pattern is acceptable.

M3 measured why: `(Rechnung|Invoice)\s*(\d+\s*)+$` blows up only on input
containing the pattern author's own literal prefix, so generic probes score it
1.0 — perfectly linear — while it kills a 26-character subject-class blowup.
Probe inputs derived from literals extracted out of the pattern itself are
permitted as additional **rejection** evidence, and are still not acceptance
evidence for a pattern the structural rules reject.

## 7. No escape hatch

There is no "I understand the risk" checkbox, no env var that disables
R1/R2/R3 or the length cap, and no per-mailbox override. A single-operator system has the
same person as author, attacker and victim; an escape hatch converts a stated
bound into a preference, and the person who flips it is the one who gets the
2 s CPU slice. An operator who genuinely needs a rejected pattern files it as a
bug and restructures in the meantime.

## 8. Outcome vocabulary and dispositions

`MatchOutcome` keeps `MATCH`, `NOMATCH`, `TIMEOUT`, `UNAVAILABLE`. The subject
length decision is made in the poller **before** the matcher is called, so the
child protocol is unchanged and gains no length outcome.

| outcome | decided by | ingestion | `\Seen` | cursor | visible as |
| --- | --- | --- | --- | --- | --- |
| `MATCH` | filter | yes | set | advances | normal path |
| `NOMATCH` | filter | no | set | advances | normal path |
| `TIMEOUT` | child deadline | no | set | advances | warning + counted |
| `SUBJECT_TOO_LONG` | length cap | no | set | advances | warning + counter |
| `INVALID_PATTERN` at save time | R1/R2/R3/bounds | n/a | untouched | untouched | dashboard error naming the rule |
| `INVALID_PATTERN` at startup (row edited by hand) | R1/R2/R3/bounds | undecided | **not set** | **frozen** | poll error, `record_poll_failure`, 3-strike path |
| `UNAVAILABLE` | child cannot run | undecided | **not set** | **frozen** | poll error, `record_poll_failure`, 3-strike path |

A stored filter that fails the contract at startup takes `UNAVAILABLE`'s
disposition rather than a silent one: the poll errors and the cursor freezes,
because the alternative is deciding mail with a rule the system has already
refused to reason about.

**Invariants that the table does not carry.**

- `SUBJECT_TOO_LONG` **never calls `record_poll_failure`**. It is a disposition,
  not a fault: it must not bump `consecutive_failures` and must not be able to
  drive a mailbox into the 3-strike disable. A hostile sender must not be able
  to switch a mailbox off by sending long subjects.
- It creates **no job** and records **nothing** in `dedup_hashes`: nothing was
  ingested, so nothing may be marked as seen-and-handled by the dedup path.
- `\Seen` and the cursor advance together, exactly as `TIMEOUT` does them
  today.

**Recovery from a disabled mailbox, stated because the code does not do it
automatically.** `record_poll_failure` sets `enabled = 0` at the third
consecutive failure (`store.py:470-476`) and `record_poll_success` resets
`consecutive_failures` and `last_error` but **never restores `enabled`**
(`store.py:455-459`). So after an operator corrects a rejected filter the
mailbox stays disabled until they re-enable it through the M4 toggle. Saving a
corrected filter clears `last_error` and `consecutive_failures` — the cause is
provably gone — but does not flip `enabled`: that flag is operator-controlled
and the dashboard must not quietly start polling something the operator turned
off.

**Parity, honestly scoped.** `subject_matches` returns a bool and therefore
cannot represent `UNAVAILABLE` or `SUBJECT_TOO_LONG` at all, so a promise that
it "mirrors every row" would be false. Instead: the *semantics* — case-insensitivity,
the empty-pattern and `None`-subject branches, the cap and its disposition —
live in one shared helper that both paths call, and the poller/child path
returns an outcome-bearing type. Pins assert the shared helper's decisions and
both §4 reproducers rewritten to expect `SUBJECT_TOO_LONG`; they do not assert a
Boolean parity that cannot exist.

## 9. Corpora, pins, and the release gate

- **Must-reject corpus — 14 fixtures, expected outcome recorded per fixture
  under the combined R1+R2+R3 policy.** The catastrophic set
  (`(a+)+$`, `(a|aa)+b`, `^(\w+\s?)*$`, `(Rechnung|Invoice)\s*(\d+\s*)+$`,
  `^(Rechnung|Invoice)[ ]*(\d+)+$`, `(a+)+\d+$`, `(x+x+)+y`,
  `(\s*\w+\s*){1,10}rechnung`, `^(a*){20}b$`, `(a?)*b`, `(?:(a)|a)+x`), the
  unsupported construct `(a)\1+rechnung`, and the two **documented costs**
  `(Re: |WG: )+` and `(yes|no)*!` — which are in this corpus on purpose, so
  that if the policy is ever relaxed the relaxation is a decision somebody had
  to make loudly rather than a silent drift.
- **Must-accept corpus — 28 fixtures.** The realistic operator patterns, each
  with `accept` recorded explicitly (no blanket statement that "the corpus
  passes"): includes `^Rechnung[- ]?(1|II)?$`, `^(Rechnung|Invoice)?$`,
  `^(?:Rechnung|Invoice)?\s*\d+$`, `^(Rechnung|Invoice){1,3}\d+$`,
  `(a|b)*x`, `(a|b)+x`, `(a?){10}b`, `(a|aa){0,1}b`. Retained as a
  **compatibility** corpus whose purpose is to make over-tightening fail
  loudly; it is not a safety corpus and must not be described as one.
- **Invalid-input fixtures.** Patterns CPython itself refuses, including `(?i)`
  anywhere but the start (measured: `re.error: global flags not at the start`),
  saved as a rejection carrying CPython's own message.
- **Accepted-but-slow corpus.** `^a*a*a*b$` and the unanchored-restart cases
  like `a+b` live here, **not** in the must-reject corpus: the structural rules
  accept them, and their cost is held by the length cap and the deadline.
  Pinned **behaviourally**, never by timing: a subject over the cap never
  reaches the matcher; a subject exactly at the cap reaches it unchanged;
  an expensive accepted pattern is evaluated in the child and is killed there.
  The §2 numbers are illustrative observations about one machine, not
  acceptance criteria, and a test that asserts "~75 ms" would fail on a slower
  laptop while passing on a machine where the guard is broken.
- **Mutation pins.** Each rule (R1, R2, R3, each analysis bound, the length
  cap) gets a pin that breaks when the rule is weakened, so deleting a rule
  turns a test red rather than silently widening the accepted set.
- **Shape pins.** Two of them. One asserting the parse-tree shapes the adapter
  reasons about (a repetition node's body is reachable; repetition and group
  bodies come back as the expected container type). One asserting the
  normalisation from §3: `(a|b)+x` produces **no** `BRANCH` node and
  `(a|aa)+b **does**. A CPython refactor must surface as a failing test, never
  as silent acceptance.
- **Shared-semantics pins** for the helper behind both paths, covering every row
  of the §8 table — including that `SUBJECT_TOO_LONG` leaves
  `consecutive_failures` untouched and writes no dedup row.
- **Per-interpreter gate.** The corpora run on **3.12 and 3.13**, which is the
  supported-runtime set for this contract even though `pyproject` declares the
  broader floor `>=3.12`; adopting a further interpreter requires re-running the
  corpora before that build ever polls mail.

## 10. Where it runs

One module owns the contract. It is called from three places, all of which must
use the same code:

1. **Dashboard save path** (M4) — refuses the write, names the rule.
2. **The killable child**, as a new `analyze` op — so the analysis itself runs
   inside the deadline, and a pathological pattern cannot hang validation the
   way `(a+)+\d+$` once hung the pre-validation probe.
3. **Startup pre-validation** of filters already in the database, for rows
   edited by hand or written by an older version; a refused stored filter
   behaves as `INVALID_PATTERN` for that mailbox rather than starting a poll
   loop that will misbehave.

Probes and the ratio gate stay where they are, re-described per §6.

## 11. Residual risk register (accepted, dated 2026-11-14)

1. Accepted patterns may be superlinear. The worst cost measured after the
   length cap was ~75 ms per message for `^a*a*a*b$`-class at the cap, on one
   machine — an **observation, not a bound**: more sequential repetitions can
   exhaust the deadline on shorter inputs, which is why §5 stays necessary.
2. Sustained hostile traffic can still burn `deadline × concurrent children` of
   CPU. The controls bound work per attempt, not aggregate capacity; the only
   aggregate bound that exists is the concurrency already in the system (§2).
3. The 2 s figure is a child-attempt bound; end-to-end poll latency under a
   stalled IMAP server is separately known to exceed Docker's stop grace and is
   tracked as M4 work.
4. `re._parser` is private; the mitigation is fail-closed traversal plus the
   shape pins, not API stability.
5. R1 rejects some non-catastrophic patterns — `(Re: |WG: )+` and `(yes|no)*!`
   are the named examples, both in the must-reject corpus so a relaxation has to
   be deliberate. Their operators lose a construct and gain a precise message.
6. Mail with subjects over 1000 code points is never auto-ingested. On this
   deployment that is an accepted ingestion limit, made countable rather than
   silent.

## 12. Deviations

- **From the M3 gate wording** ("conservative accepted subset"): replaced by the
  layered heuristic of §1, with the guarantee restated as bounded work per
  message. This is the deliberate substitution §1 asks to be read as.
- **From `spec.md` §5.6 / §5.7**: the spec describes validation as "reject
  patterns that show super-linear backtracking on sample input", i.e. probe
  based. §6 forbids probes as acceptance evidence, so the spec's wording is
  superseded: probes refuse, structure accepts, and the length cap plus the
  child deadline bound the cost.
- **From shipped M3 behaviour**: `MAX_SUBJECT_CHARS` changes from truncation to
  `SUBJECT_TOO_LONG` (§4), on the evidence that truncation is wrong in both
  directions.
- **Supported runtimes**: this contract tests 3.12 and 3.13, which is narrower
  than `pyproject`'s declared `>=3.12`. The declared floor is not raised here,
  but no interpreter outside the tested pair may poll mail before the corpora
  have been run on it (§9).

## 13. Out of scope

**New** aggregate admission control, rate limiting, a token bucket over filter
evaluation, or a global matcher concurrency cap — the concurrency limits already
in the system remain operative and are deliberately left alone, so nothing here
should be read as a service-availability guarantee. Also out of scope: a
load-dependent second, shorter deadline tier for oversized subjects (it would
still skip mail, and its outcomes would depend on load); a complexity-guaranteed
regex grammar; making the length cap configurable; a raw-header length shortcut
(rejected in §4 because encoded-word whitespace makes inflation unbounded);
branch-overlap analysis (§3); `regex`-module timeouts (a new dependency, and
`spec.md` §7 pins `regex` for a different reason); changing `\Seen`/cursor
semantics for `TIMEOUT`.
