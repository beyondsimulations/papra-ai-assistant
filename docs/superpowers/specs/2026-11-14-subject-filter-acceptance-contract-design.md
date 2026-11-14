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
no tractable structural rule bounds matching *complexity*:

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
| exponential backtracking | `(a+)+$` | **26 characters** → 2.50 s | R1/R2 only — the length cap cannot help, a subject is longer than 26 chars |
| polynomial, sequential repeats | `^a*a*a*b$` | 1 000 chars → 0.074 s; 10 000 → >8 s | length cap only — no structural rule rejects it |
| polynomial, search restarts | `a+b` (unanchored) | 60 000 → 0.77 s; 250 000 → >8 s | length cap only |
| anything unforeseen | — | — | child deadline, §5 |

Stated positively: **a hostile message cannot burn more than one child
deadline per message per worker, and a match cannot outlive its child.**

Stated negatively, and this is load-bearing prose:

- `SUBJECT_FILTER_TIMEOUT_S` bounds **one match attempt inside one child**. It
  is not a bound on poll latency, which also contains IMAP round-trips, Papra
  uploads, retries, IPC and process cleanup.
- It is not an aggregate capacity guarantee. N children at 2 s each are N
  busy cores, and sustained hostile traffic can saturate a home server while
  every individual deadline is honoured.
- A length cap of 1000 and a structural rule set neither establish that an
  accepted pattern is cheap. More sequential repetitions exhaust the budget on
  short inputs. The deadline stays necessary because of what the other two do
  not prove.

## 3. Control 1 — structural acceptance

Two rejection rules, evaluated over the parse tree, not over source text
(CPython factors common prefixes and rewrites alternations into character
classes, so source-level rules would misjudge what the engine actually does).

**R1 — a repetition whose body contains another repetition is rejected.**
Covers nested quantifiers and quantified-backreference-style blowups.
Measured against 8 catastrophic patterns: 7 rejected, including
`(Rechnung|Invoice)\s*(\d+\s*)+$`, which scored **1.0** on the growth-ratio
probe and was therefore accepted by M3's validation. Against 20 realistic
operator patterns: 0 false rejections.

**R2 — a repetition whose body contains an alternation is rejected.**
Closes R1's single measured miss, `(a|aa)+b`, where the ambiguity lives in one
quantified group's branches and there is no nested repetition to see.

R2 is deliberately blunt and its cost is accepted knowingly: it rejects
`(Re: |WG: )+`, which is **not** inherently catastrophic — its branches have
distinct first characters and fixed lengths. That is conservatism, not danger,
and an operator told "a quantified group may not contain `|`" can restructure.
The sharper alternative (reject only when branches overlap) was rejected as the
start of a language-automata checker we could not verify.

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

The subject is bounded by §4 and the IPC payload by the existing message cap.
These bounds protect the analyser; they are not what bounds matching cost.

**Fail closed, and distinguish two kinds of "no".** The traversal is
opcode-specific: it recognises the nodes it reasons about and treats anything
unfamiliar — an unexpected opcode, an unrecognised child shape, a parse error,
an analysis that runs out of its own budget — as a **rejection of that
pattern**, never as acceptance. `re._parser` is a private module and the
project declares `>=3.12` rather than one interpreter, so it sits behind a thin
adapter whose only job is to make the shape assumptions explicit and to fail
closed when they stop holding.

A **pattern-level** rejection (invalid, or refused by R1/R2/bounds) means: the
filter is not saved, the operator is told which rule fired and how to
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
  status page. Storage is M4's choice; the requirement is that skipping is
  countable rather than log-scrapable.
- **No filter configured → rule does not apply.** No filter means ingest;
  there is nothing to refuse.

`SUBJECT_TOO_LONG` is **not** evidence that the subject failed the regex. It is
a processing-domain restriction, and no dashboard badge may imply "did not
match".

**Measurement point.** A raw `Subject` header value longer than 32 768
characters is `SUBJECT_TOO_LONG` without decoding it. That shortcut has to be
provably free of false `SUBJECT_TOO_LONG`, so its arithmetic is stated rather
than assumed: the maximally inflated header packs one 4-byte code point per
encoded word (`=?UTF-8?Q?=F0=9F=98=80?=` plus the CRLF and space a folded header
needs), which measured **27 raw characters per decoded code point**, so 32 768
raw characters decode to **at least 1213 code points** — over the cap by 21%,
and a pin builds exactly that maximally inflated header to hold the ratio. What
the shortcut protects is the decoder's allocation, not the message buffer: that
is already bounded by `MAX_MESSAGE_BYTES` (25 MiB) and `MAX_POLL_BYTES`
(100 MiB), checked on `RFC822.SIZE` before the fetch.

Counted as decoded Unicode code points of the value
`_decode_header_value` returns, after `decode_header`/`make_header`, before any
whitespace collapsing — and the matching path receives that same string with no
truncation anywhere.

The cap is one fixed named constant, 1000 decoded code points. It is a product
policy choice, not an RFC-derived safety threshold — RFC 5322 sets no subject
limit at all. Making it configurable would multiply the corpus and let the
operator widen the very thing the control exists to narrow.

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

There is no "I understand the risk" checkbox, no env var that disables R1/R2 or
the length cap, and no per-mailbox override. A single-operator system has the
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
| `INVALID_PATTERN` at save time | R1/R2/bounds | n/a | untouched | untouched | dashboard error naming the rule |
| `INVALID_PATTERN` at startup (row edited by hand) | R1/R2/bounds | undecided | **not set** | **frozen** | poll error, `record_poll_failure`, 3-strike path |
| `UNAVAILABLE` | child cannot run | undecided | **not set** | **frozen** | poll error, `record_poll_failure`, 3-strike path |

A stored filter that fails the contract at startup takes `UNAVAILABLE`'s
disposition rather than a silent one: the poll errors and the cursor freezes,
because the alternative is deciding mail with a rule the system has already
refused to reason about. The mailbox stops polling through the existing
3-strike path and the operator sees why.

`subject_matches`, the synchronous reference implementation, mirrors all of it
so the two paths cannot drift; the pins below assert the parity.

## 9. Corpora, pins, and the release gate

- **Must-reject corpus.** The 8 catastrophic patterns used for R1/R2, plus
  quantified alternations, patterns above each of the four analysis bounds,
  inputs that are not valid regexes (including `(?i)` written anywhere but the
  start, which CPython itself refuses — a save-time rejection carrying
  CPython's own message), and assertion-bearing and huge-bounded-repeat
  variants.
- **Must-accept corpus.** The 20 realistic patterns, retained as a
  **compatibility** corpus: its purpose is to make over-tightening fail loudly.
  It is not, and must not be described as, a safety corpus.
- **Accepted-but-slow corpus.** `^a*a*a*b$` and the unanchored-restart cases
  like `a+b` live here, **not** in the must-reject corpus: R1/R2 accept them,
  and the point of pinning them is that they are accepted *and* that their
  cost is held by the length cap and the deadline. A test asserts the measured
  shape from §2 (superlinear beyond the cap, ~75 ms at the cap) so that if the
  length cap is ever removed these fail loudly instead of becoming an
  availability hole.
- **Mutation pins.** Each rule (R1, R2, each bound, the length cap, the
  raw-header shortcut) gets a pin that breaks when the rule is weakened, so
  deleting a rule turns a test red rather than silently widening the accepted
  set.
- **Shape pin.** A test asserting the parse-tree shapes the adapter reasons
  about (that a repetition node's body is reachable, that group and repetition
  bodies come back as the expected container type). A CPython refactor must
  surface as a failing test, never as silent acceptance.
- **Parity pins** between `subject_matches` and `KillableMatcher` on every row
  of the §8 table, including both truncation reproducers from §4 rewritten to
  assert `SUBJECT_TOO_LONG` instead.
- **Per-interpreter gate.** The corpora are re-run on every interpreter we
  support (3.12, 3.13) as part of the release check, and adopting a new
  interpreter requires re-running them before the first poll on it.

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

1. Accepted patterns may be superlinear; worst measured cost after the length
   cap is ~75 ms per message for `^a*a*a*b$`-class at the cap.
2. Sustained hostile traffic can still burn `deadline × workers` CPU; the
   controls bound work per message, not aggregate capacity.
3. The 2 s figure is a child-attempt bound; end-to-end poll latency under a
   stalled IMAP server is separately known to exceed Docker's stop grace and is
   tracked as M4 work.
4. `re._parser` is private; the mitigation is fail-closed traversal plus the
   shape pin, not API stability.
5. R2 rejects some non-catastrophic patterns. Their operators lose a construct
   and gain a precise message.
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

## 13. Out of scope

Aggregate rate limiting or a token bucket over filter evaluation; a
load-dependent second, shorter deadline tier for oversized subjects (it would
still skip mail, and its outcomes would depend on load); a
complexity-guaranteed regex grammar; making the length cap configurable;
`regex`-module timeouts (a new dependency, and `spec.md` §7 pins `regex` for a
different reason); changing `\Seen`/cursor semantics for `TIMEOUT`.
