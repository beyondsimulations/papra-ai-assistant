# Subject-Filter Acceptance Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the unbounded regex validator with a fail-closed static analyzer (rules R1–R4), stop truncating subjects and start retiring over-long ones as `SUBJECT_TOO_LONG`, give one matcher one message-wide deadline shared across its retries, and freeze the accepted language as corpora of record that run on every interpreter we support.

**Architecture:** Three layers, each with one job. `papra_suite/regexlint.py` is pure static analysis of a pattern — it imports `re._parser`, refuses everything it cannot positively prove safe, and knows nothing about mail or processes. `papra_suite/safematch.py` + `papra_suite/regex_worker.py` stay the killable transport: the analyzer runs *inside* the child, so a pathological parse is killed like any other work, and the transport now accounts for one deadline per message rather than per attempt. `papra_suite/mail.py` owns the policy that is not about regexes: the two length caps, the `SUBJECT_TOO_LONG` disposition, and the single admission boundary both poll paths must pass.

**Tech Stack:** Python 3.12, stdlib only (`re`, `re._parser`, `signal`, `sqlite3`, `asyncio`). No new dependencies. New: two corpus data files, one checker script, one multi-interpreter gate script.

**Spec:** `docs/superpowers/specs/2026-11-14-subject-filter-acceptance-contract-design.md` — approved 2026-11-14. Read §2 (the guarantee), §3 + §3.1 (the rules and traversal), §4 (the two caps), §5 (layering), §6 (the child, the budget, caching), §8 (dispositions and counters), §9 (the pin sets), §11 (residuals), Appendix A (the corpora). This plan argues from that document; both travel together.

## Global Constraints

- Python 3.12 floor; `re._parser` is private API, which is precisely why traversal and the opcode table are fail-closed (spec §3.1, §11.7). **Do not add a dependency and do not vendor `regex`.**
- Every task ends green: `.venv/bin/ruff check .` clean, `.venv/bin/pytest -q -m "not integration"` green. Docker stack is not required by any task in this plan.
- Fail-closed rule for this whole plan: when the analyzer cannot prove an answer, it **rejects**. "Unknown width" means reject, never "treat as fixed"; an unrecognized node means reject, never "skip". A defect in a security check must degrade to a refusal.
- One terminal disposition per message per pass (spec §8): a message is at most one of ingested / filtered / `SUBJECT_TOO_LONG` / `TIMEOUT`, and that holds across the retry loop.
- Counting is at-least-once: never claim a counter is exact (spec §8.2).
- The analyzer must never run on the event loop: it lives in the child (spec §5.1). `regexlint.analyze` is sync and CPU-only, never async, never logging.
- The worker's stdout is the protocol channel and nothing else (spec §5.2). Any new reply kind needs its own pinned wire format.
- Commit after every task with the message shown. Never push.

## File Structure

| File | Responsibility |
|---|---|
| Create `papra_suite/regexlint.py` | Static analysis only: fail-closed traversal of the parse tree, the width/nullable model, rules R1–R4, size bounds, `ANALYSIS_POLICY_VERSION`. No mail, no processes, no I/O. |
| Create `tests/data/subject_filter_must_reject.txt`, `tests/data/subject_filter_must_accept.txt` | The corpora of record (spec §9). One pattern per line, `#` comments. Read by both the tests and the checker script — one copy, no drift. |
| Create `tests/test_regexlint.py` | Rule witnesses, structural/mutation pins, traversal and counting pins. |
| Modify `papra_suite/regex_worker.py` | `analyze` op, the `ANALYSIS_TIMEOUT` frame, exit-code semantics for a blown analysis deadline. |
| Modify `papra_suite/safematch.py` | `KillableMatcher.analyze`, one message-wide deadline across retries, child-alarm-death classification. |
| Modify `papra_suite/mail.py` | `MAX_RAW_SUBJECT_HEADER_CHARS`, parse-level `subject_too_long`, no truncation, `SUBJECT_TOO_LONG` disposition, `subject_filter_admission` + cached identity, ratio-gate removal. |
| Modify `papra_suite/models.py`, `papra_suite/store.py` | `Mailbox.subject_too_long_count`, migration 002, `Store.record_subject_too_long`. |
| Modify `papra_suite/pipeline.py` | Both poll paths use the same admission boundary. |
| Create `scripts/filter_contract_check.py`, `scripts/filter_contract_gate.sh` | Corpora runner for a dashboard save pre-flight; per-interpreter gate (spec §9). |

**Ordering.** Tasks 1–4 build the analyzer (each rule with its own witnesses), 5–6 make the transport honest about it, 7–8 move policy into `mail.py`, 9 freezes the language. Do not reorder 5 before 1–4: the `analyze` op has to have something to run.

---

### Task 1: Fail-closed parse adapter and bounds

**The node model, measured — read this before writing any traversal.** Everything
below was verified on CPython 3.12.15 by dumping `re._parser.parse` output; the
first draft of this plan assumed a different model and was wrong in four ways, each
of which would have produced an analyzer that quietly certified hostile patterns:

- `parse()` returns a `re._parser.SubPattern`, which **has no `.op` and no
  `.items`**. Its payload is `.data`, a list of `(opcode, payload)` pairs.
- `SubPattern` **is** indexable: `__getitem__`/`__len__` expose `data`, and
  slicing returns a *new* `SubPattern`. So iterating one yields `(opcode, payload)`
  pairs — neither a plain list of children nor an opaque object.
- An opcode is an **enum member identified by `.name`** (`"MAX_REPEAT"`,
  `"BRANCH"`, `"LITERAL"`), not an int. A table keyed by integers matches nothing
  and fails silently.
- A repetition's payload is `(min, max, body)` for **all four** repeat opcodes, and
  `max` is the `MAXREPEAT` sentinel rather than a number. Reading `payload[0]` as
  the body yields an int, so the walk finds no children and reports `(a+)+` safe.
  This single trap is the reason Task 1 exists on its own.

Also measured: `re._parser.FLAGS` is the flag-letter **dict**, not leaked state, so
no global reset is needed — pass `flags` to `parse()` instead. A single-character
alternation is compiled into a character class (`(a|b)+` yields `IN`, no `BRANCH`),
which is why `(a|b)+x` is accepted and documented as superlinear-but-cheap at the
cap. `x{2}*` is a **parse error** ("multiple repeat"), so it cannot serve as a
counting witness. 3.12 has `ATOMIC_GROUP` and `POSSESSIVE_REPEAT`, and this module
refuses both as unknown opcodes.

**Files:**
- Create: `papra_suite/regexlint.py`
- Test: `tests/test_regexlint.py`

**Interfaces:**
- Consumes: `re`, `re._parser` (stdlib).
- Produces (Tasks 2–5 and the worker depend on these exact names):

```python
ANALYSIS_POLICY_VERSION = 1        # bump on ANY rule/bound/traversal change (§6.3)
MAX_PATTERN_CHARS = 512            # the four bounds are spec §3's policy numbers,
MAX_NODES = 512                    # copied verbatim: 512 chars, 512 nodes,
MAX_DEPTH = 24                     # depth 24, 64 repetition nodes
MAX_REPETITIONS = 64
ANALYSIS_MIN_S = 0.05              # Task 5: floor for a usable analysis clock
ANALYSIS_TIMEOUT_FALLBACK_S = 0.25

class AnalysisTimeout(Exception): ...   # the analyzer ran out of ITS budget

@dataclass(frozen=True)
class Analysis:
    ok: bool
    reason: str = ""               # operator-facing; the dashboard renders it verbatim

def analyze(pattern: str, flags: int = 0, *, deadline_s: float = 0.25) -> Analysis
    # never raises except AnalysisTimeout
```

- [ ] **Step 1: Write the failing tests — the adapter, not the rules**

```python
# tests/test_regexlint.py
"""Static subject-filter analysis: rules R1-R4 plus the pins that keep them honest.

Every structural test here exists because the first draft of the analyzer got that
shape wrong. They are not decoration: a wrong assumption in the adapter makes the
rules blind rather than wrong, which is the worse failure.
"""
import re
import re._parser as _parser
import time

import pytest

from papra_suite import regexlint
from papra_suite.regexlint import Analysis, AnalysisTimeout, analyze


class TestNodeModel:
    def test_subpattern_is_indexable_and_yields_opcode_payload_pairs(self):
        tree = _parser.parse("(a+)+", 0)
        assert len(tree) == 1
        assert regexlint._is_node(tree[0])
        # slicing returns a SubPattern, NOT a list: a walk that treats it as a
        # list of children would work here and break somewhere else
        assert isinstance(tree[0:1], _parser.SubPattern)

    def test_opcode_is_an_enum_member_identified_by_name_not_an_int(self):
        # An int-keyed opcode table would match nothing and stay silent about it.
        node = _parser.parse("a+", 0)[0]
        assert node[0].name == "MAX_REPEAT"

    def test_repetition_payload_is_min_max_body_and_reading_index_zero_finds_no_body(self):
        # THE trap. payload[0] is the int `min`; a walk that reads it as the body
        # descends into nothing and certifies a catastrophic pattern as safe.
        node = _parser.parse("a{2,3}", 0)[0]
        assert node[1][0] == 2 and node[1][1] == 3
        assert not regexlint._is_node(node[1][0])
        guard = regexlint._Guard(time.monotonic() + 5)
        regexlint._walk(regexlint._nodes(_parser.parse("a{2,3}", 0)), 0, guard)
        assert guard.nodes == 2          # MAX_REPEAT + LITERAL: the body was reached

    def test_max_uses_the_maxrepeat_sentinel(self):
        node = _parser.parse("a*", 0)[0]
        assert regexlint._is_max(node[1][1])
        assert not regexlint._is_max(node[1][1 - 1])   # min is an int

    def test_single_char_alternation_becomes_a_class_not_a_branch(self):
        # Documented consequence: R2 cannot see `(a|b)+` because there is no
        # BRANCH to see. It is fixed-width per repetition and cheap at the cap
        # (0.005 s for `(a|b)+x` at 1 000 characters), so it stays ACCEPTED and
        # is listed in the must-accept corpus (spec §9, Appendix A.2).
        assert analyze("(a|b)+x", re.IGNORECASE).ok

    def test_a_payload_shape_that_is_not_read_is_rejected_not_walked_past(self):
        class Opaque:
            def __len__(self): return 1
            def __getitem__(self, i): return (None,)
        node = (_parser.LITERAL, Opaque())
        assert regexlint._unconsumed(node)
        with pytest.raises(regexlint._Reject):
            regexlint._walk([node], 0, regexlint._Guard(time.monotonic() + 5))

    def test_unknown_opcode_rejects_instead_of_being_skipped(self, monkeypatch):
        monkeypatch.setattr(regexlint, "_KNOWN_OPS", frozenset())
        assert "not understood" in analyze("^abc$", re.IGNORECASE).reason

    def test_atomic_group_and_possessive_are_refused_as_unknown(self):
        # 3.12 has ATOMIC_GROUP / POSSESSIVE_REPEAT. They are arguably safe (no
        # backtracking), but this module refuses what it has not reasoned about,
        # which is a real and documented cost to the operator.
        assert "not understood" in analyze("^(?>a*)$", re.IGNORECASE).reason
        assert "not understood" in analyze("^a*+$", re.IGNORECASE).reason


class TestBoundsAndDeadlines:
    def test_pattern_length_bound(self):
        assert "512 characters" in analyze("^" + "a" * 600 + "$", re.IGNORECASE).reason

    def test_node_depth_and_repetition_bounds(self, monkeypatch):
        # The bounds are policy numbers, so they are tested by shrinking the
        # constant rather than by manufacturing a 512-node pattern. The accepted
        # corpus measures 31 nodes / depth 4 / 4 repetitions (spec §3).
        monkeypatch.setattr(regexlint, "MAX_NODES", 5)
        assert "parse nodes" in analyze("^abcdefgh$", re.IGNORECASE).reason
        monkeypatch.setattr(regexlint, "MAX_NODES", 512)
        monkeypatch.setattr(regexlint, "MAX_DEPTH", 3)
        assert "nesting depth" in analyze("((((a))))", re.IGNORECASE).reason
        monkeypatch.setattr(regexlint, "MAX_DEPTH", 24)
        monkeypatch.setattr(regexlint, "MAX_REPETITIONS", 2)
        # three repetitions in three SEPARATE top-level groups: counted across the
        # forest, not per subtree (counting the largest subtree would find 1)
        assert "repetition nodes" in analyze("^(?:a+)(?:b+)(?:c+)$", re.IGNORECASE).reason

    def test_deadline_raises_and_is_not_a_verdict(self):
        with pytest.raises(AnalysisTimeout):
            analyze("^" + "(" * 40 + "a" + ")" * 40 + "$", re.IGNORECASE, deadline_s=0.0)

    def test_compile_error_and_empty_pattern_reject_with_their_own_reasons(self):
        assert "invalid regular expression" in analyze("x{2}*", re.IGNORECASE).reason
        assert "empty pattern" in analyze("", re.IGNORECASE).reason

    def test_unexpected_errors_fail_closed_with_a_named_reason(self, monkeypatch):
        def explode(*args, **kwargs):
            raise RuntimeError("boom")
        monkeypatch.setattr(regexlint, "_walk", explode)
        got = analyze("^abc$", re.IGNORECASE)
        assert not got.ok and "failed closed" in got.reason and "RuntimeError" in got.reason
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/bin/pytest -q tests/test_regexlint.py`
Expected: collection error, `ModuleNotFoundError: No module named 'papra_suite.regexlint'`.

- [ ] **Step 3: Write the module — adapter, traversal, bounds. No rules yet.**

```python
# papra_suite/regexlint.py
"""Static analysis of an operator-supplied subject filter: reject by SHAPE.

WHY SYNTACTIC. A regex's worst case cannot be measured: a probe can only reach
the backtracking its own input can drive, so a timing gate on short strings
accepts an exponential pattern (spec Appendix A is measured). What CAN be decided
cheaply is whether the pattern is built from constructs whose worst case is
unbounded. Four rules do that on the parse tree, and every one is a syntactic
approximation whose complement is open — an accepted pattern may still be slow,
and the killable child plus its deadline are what make the next missed class
survivable rather than fatal (spec §11.9).

FAIL CLOSED, EVERYWHERE. `re._parser` is private API whose node shapes are an
implementation detail, and the project floor is 3.12 while the analyzer runs on
whatever interpreter the deployment has. So nothing here trusts a shape: a node
that is not a 2-element (opcode, payload) pair is a REJECTION, an opcode not in
_KNOWN_OPS is a REJECTION, a payload container this module did not read is a
REJECTION, an unknown width is a REJECTION. A defect in this module must degrade
to refusing a pattern an operator would have grumbled about, never to certifying
a hostile one. The shapes themselves are pinned in tests/test_regexlint.py,
because a wrong assumption here makes the rules BLIND rather than wrong.

THE GUARD IS A DEADLINE PLUS SIZE, NOT A BOUND ON WORK. Traversal runs over
operator-chosen input, so node/depth/repetition counts are bounded fail-closed
AND a wall-clock deadline aborts mid-walk. The two outcomes stay distinct:
AnalysisTimeout means "the analyzer was too slow" (retried once, Task 5);
Analysis(ok=False) means "this pattern is refused".
"""

from __future__ import annotations

import re
import re._parser as _parser
import time
from dataclasses import dataclass

# Bump on ANY change to the rules, bounds, or traversal: a cached judgement made
# under a different version must not be reusable (spec §6.3).
ANALYSIS_POLICY_VERSION = 1

MAX_PATTERN_CHARS = 512
MAX_NODES = 512
MAX_DEPTH = 24
MAX_REPETITIONS = 64
ANALYSIS_MIN_S = 0.05
ANALYSIS_TIMEOUT_FALLBACK_S = 0.25

_REPEAT_OPS = frozenset({"MAX_REPEAT", "MIN_REPEAT", "REPEAT_ONE", "MIN_REPEAT_ONE"})
_FIXED_ONE = frozenset({
    "LITERAL", "NOT_LITERAL", "CATEGORY", "IN", "RANGE", "ANY", "ANY_ALL",
    "ANY_NONE", "ANY_IGNORE_ALL", "NEGATE",
})
_ZEROWIDTH = frozenset({"AT", "ASSERT", "ASSERT_NOT", "MARK"})
# Ops whose payload nests subpatterns beyond the generic "every element is a
# node" rule.
_ASSERT_OPS = frozenset({"ASSERT", "ASSERT_NOT", "GROUPREF_EXISTS"})
# Everything this module can account for. An interpreter that adds an opcode
# cannot slip past the gate unnoticed, because anything else is rejected.
_KNOWN_OPS = frozenset({
    "LITERAL", "NOT_LITERAL", "CATEGORY", "IN", "RANGE", "ANY", "ANY_ALL",
    "ANY_NONE", "ANY_IGNORE_ALL", "NEGATE", "AT", "MARK", "SUCCESS", "FAILURE",
    "ASSERT", "ASSERT_NOT", "SUBPATTERN", "BRANCH", "MAX_REPEAT", "MIN_REPEAT",
    "REPEAT_ONE", "MIN_REPEAT_ONE", "GROUPREF", "REF", "GROUPREF_EXISTS",
})


class AnalysisTimeout(Exception):
    """The analysis ran out of its own deadline. Not a verdict about the pattern."""


@dataclass(frozen=True)
class Analysis:
    ok: bool
    reason: str = ""


class _Reject(Exception):
    """Internal: a rule or a bound tripped. Carries the operator-facing reason."""


class _Guard:
    """Node/depth counters and the wall-clock deadline, checked at every node."""

    def __init__(self, deadline: float) -> None:
        self._deadline = deadline
        self.nodes = 0
        self.repetitions = 0

    def tick(self, depth: int) -> None:
        self.nodes += 1
        if self.nodes > MAX_NODES:
            raise _Reject(f"pattern exceeds {MAX_NODES} parse nodes")
        if depth > MAX_DEPTH:
            raise _Reject(f"pattern exceeds nesting depth {MAX_DEPTH}")
        if time.monotonic() >= self._deadline:
            raise AnalysisTimeout("analysis deadline exceeded")


def _seq(x) -> list:
    """The elements of a parse container.

    SubPattern is indexable but is not a list; iterating it yields its
    (opcode, payload) pairs and slicing returns a new SubPattern. Lists, tuples,
    and SubPatterns all go through here; strings and bytes are payloads, not
    containers.
    """
    if isinstance(x, (str, bytes)):
        return []
    if isinstance(x, (list, tuple)):
        return list(x)
    if hasattr(x, "__len__") and hasattr(x, "__getitem__"):
        return [x[i] for i in range(len(x))]
    return []


def _is_node(x) -> bool:
    """A parsed instruction is a 2-element (opcode, payload) pair and the opcode
    is an enum member with a .name, not an int."""
    return isinstance(x, (tuple, list)) and len(x) == 2 and hasattr(x[0], "name")


def _name(node) -> str:
    return node[0].name


def _payload(node):
    return node[1]


def _nodes(x) -> list:
    return [c for c in _seq(x) if _is_node(c)]


def _is_max(value) -> bool:
    """The unbounded marker is the MAXREPEAT sentinel, not a number."""
    return getattr(value, "name", None) == "MAXREPEAT"


def _repeat_bounds(node):
    """(min, max or None) for a repetition, or a rejection.

    Proves the payload shape BEFORE anything reads it, because the shape is the
    trap: a repetition is (min, max, body) and reading element 0 as the body
    finds an int, then no children, then a clean bill of health for `(a+)+`.
    """
    payload = _payload(node)
    if not isinstance(payload, (tuple, list)) or len(payload) != 3:
        raise _Reject("repetition payload is not (min, max, body)")
    low, high = payload[0], payload[1]
    if not isinstance(low, int):
        raise _Reject("repetition minimum is not an integer")
    if _is_max(high):
        return low, None
    if not isinstance(high, int):
        raise _Reject("repetition maximum is neither an integer nor unbounded")
    return low, high


def _children(node) -> tuple[list, set]:
    """(nested nodes, ids of the payload elements that were read).

    The second value is what makes this fail closed: _unconsumed() asks whether
    anything container-shaped was left unread.
    """
    name = _name(node)
    payload = _payload(node)
    if name in _REPEAT_OPS:
        _repeat_bounds(node)                      # proves the shape before [2]
        return _nodes(payload[2]), {id(payload[2])}
    if name == "SUBPATTERN":
        if len(payload) < 4:
            raise _Reject("group payload is not (group, add, name, body)")
        return _nodes(payload[3]), {id(payload[3])}
    if name == "BRANCH":
        arms = payload[1] if len(payload) > 1 else []
        out: list = []
        for arm in _seq(arms):
            out += _nodes(arm)
        return out, {id(arms)}
    if name in _ASSERT_OPS:
        out, consumed = [], set()
        for part in _seq(payload):
            if _is_node(part) or (hasattr(part, "__len__")
                                  and not isinstance(part, (str, bytes))):
                out += _nodes(part)
                consumed.add(id(part))
        return out, consumed
    return _nodes(payload), {id(c) for c in _seq(payload) if _is_node(c)}


def _unconsumed(node) -> bool:
    """True when a payload element still holds structure we did not descend into."""
    _, consumed = _children(node)
    for element in _seq(_payload(node)):
        if id(element) in consumed or _is_node(element):
            continue
        if element is None or isinstance(element, (str, bytes, int, float, complex, bool)):
            continue
        if hasattr(element, "__len__") and hasattr(element, "__getitem__"):
            return True                       # an unread container: fail closed
    return False


def _walk(items, depth: int, guard: _Guard) -> None:
    """Total traversal, fail closed (spec §3.1). Tasks 2-4 attach rules here."""
    for item in items:
        if not _is_node(item):
            raise _Reject("pattern contains a node this analyzer does not recognise")
        guard.tick(depth)
        name = _name(item)
        if name not in _KNOWN_OPS:
            raise _Reject(f"opcode {name!r} is not understood")
        if _unconsumed(item):
            raise _Reject("pattern nests a payload shape this analyzer does not understand")
        if name in _REPEAT_OPS:
            guard.repetitions += 1
            if guard.repetitions > MAX_REPETITIONS:
                raise _Reject(f"pattern exceeds {MAX_REPETITIONS} repetition nodes")
        _walk(_children(item)[0], depth + 1, guard)


def analyze(pattern: str, flags: int = 0, *, deadline_s: float = 0.25) -> Analysis:
    """Decide whether a subject filter may be saved. Raises only AnalysisTimeout."""
    if len(pattern) > MAX_PATTERN_CHARS:
        return Analysis(False, f"pattern exceeds {MAX_PATTERN_CHARS} characters")
    try:
        re.compile(pattern, flags)                  # syntax errors carry re's own text
    except re.error as error:
        return Analysis(False, f"invalid regular expression: {error}")
    try:
        if not pattern:
            return Analysis(False, "empty pattern: use no filter instead")
        # flags go to parse(): the filter is COMPILED with re.IGNORECASE in the
        # poller, and an analysis that parsed with different flags would not be
        # analysing the filter that runs. (re._parser.FLAGS is the flag-letter
        # dict, not process-global state, so there is nothing to reset.)
        forest = _nodes(_parser.parse(pattern, flags))
        _walk(forest, 0, _Guard(time.monotonic() + deadline_s))
    except AnalysisTimeout:
        raise
    except _Reject as reject:
        return Analysis(False, str(reject))
    except Exception as error:  # noqa: BLE001 - a broken analyzer must refuse, never certify
        return Analysis(False, f"analysis failed closed: {type(error).__name__}: {error}")
    return Analysis(True)
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest -q tests/test_regexlint.py`
Expected: all pass. If `test_repetition_payload_is_min_max_body_and_reading_index_zero_finds_no_body` reports 1 node, the walk is not descending into the repeat body and every later rule would be blind.

- [ ] **Step 5: Confirm the whole suite is green, then commit**

Run: `.venv/bin/ruff check . && .venv/bin/pytest -q -m "not integration"`

```bash
git add papra_suite/regexlint.py tests/test_regexlint.py
git commit -m "feat(filter): fail-closed parse adapter for the subject-filter analyzer

The node model, because a wrong assumption here makes the rules blind rather
than wrong: SubPattern has no .op and no .items (its payload is .data), it IS
indexable and slicing returns a SubPattern, opcodes are enum members read by
.name rather than ints, and a repetition's payload is (min, max, body) with
MAXREPEAT as a sentinel — so a walk that reads payload[0] as the body descends
into nothing and certifies (a+)+ as safe. Each of those is pinned.

Everything unrecognised is a rejection: an opcode outside _KNOWN_OPS, a node
that is not a (opcode, payload) pair, a payload container this module did not
read. _children returns what it consumed precisely so _unconsumed can ask.

Bounds are spec §3's policy numbers (512 chars, 512 nodes, depth 24, 64
repetitions), checked at every node beside a wall-clock deadline. The deadline
is a distinct outcome from a rejection: one is retried, the other is the answer.

Measured side effects worth stating: re._parser.FLAGS is the flag-letter dict so
there is no leaked state to reset; a single-character alternation compiles to a
character class, which is why (a|b)+x is accepted; x{2}* is a parse error; and
3.12's ATOMIC_GROUP / POSSESSIVE_REPEAT are refused as unknown opcodes, which is
a real cost to an operator who writes a*+."
```

---

### Task 2: The width model and rule R1

**Files:**
- Modify: `papra_suite/regexlint.py`
- Test: `tests/test_regexlint.py`

**Interfaces:**
- Produces:

```python
def _sum_widths(widths: list) -> tuple[int, int | None] | None
def _width(node, depth: int = 0) -> tuple[int, int | None] | None
    # (min, max); max None means unbounded; a bare None means UNKNOWN and every
    # caller must treat that as a rejection
```

- [ ] **Step 1: Put the corpora of record in place, then write the failing tests**

The first rule is the first thing that needs the accepted language written down, so
the corpora arrive here rather than in Task 9. Copy every pattern from the spec's
Appendix A.1 (29) and A.2 (39) into two data files **verbatim** — the file *is* the
contract, so re-typing is a defect — and add the loader the rest of the plan reads.

```
tests/data/subject_filter_must_reject.txt   # one pattern per line; # comments group them
tests/data/subject_filter_must_accept.txt
```

```python
# tests/test_filter_contract.py
"""The accepted language is a contract with consequences, so it is a file (spec §9).

M2 shipped `(\w+\s?)*` as a README example, which is what "the accepted set is
whatever the probe happened not to catch" bought. These two files are read by the
tests here, by scripts/filter_contract_check.py (Task 9), and by the dashboard's
save pre-flight — one copy, no drift.
"""
from pathlib import Path

_DATA = Path(__file__).parent / "data"


def _read(name: str) -> list[str]:
    return [line.strip() for line in (_DATA / name).read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")]


MUST_REJECT = _read("subject_filter_must_reject.txt")
MUST_ACCEPT = _read("subject_filter_must_accept.txt")
```

Now the failing tests.

```python
class TestWidthModel:
    def test_unbounded_member_makes_the_sum_unbounded_rather_than_raising(self):
        # The bug this pins: summing (1, None) raised TypeError, which the
        # fail-closed wrapper turned into a rejection — right verdict, wrong
        # reason, and the rule that should have fired never did.
        assert regexlint._sum_widths([(1, None), (2, 2)]) == (3, None)
        assert regexlint._sum_widths([(1, 1), (2, 3)]) == (3, 4)
        assert regexlint._sum_widths([None]) is None

    def test_no_rejection_in_the_corpus_comes_from_an_accident(self):
        # Every rejection must name a rule or a bound, never "failed closed".
        # A TypeError escaping into the wrapper means a rule did not fire.
        from tests.test_filter_contract import MUST_ACCEPT, MUST_REJECT

        accidents = [p for p in MUST_ACCEPT + MUST_REJECT
                     if "failed closed" in regexlint.analyze(p, re.IGNORECASE).reason]
        assert accidents == []

    def test_branch_width_is_per_branch_not_summed_across_alternatives(self):
        # A previous draft summed across alternatives, which made (a|aa) look
        # fixed-width and let R1 pass the class it existed for. This asserts the
        # numbers on a real parse rather than on a hand-built node.
        def branch(pattern):
            for node in regexlint._nodes(_parser.parse(pattern, 0)):
                if regexlint._name(node) == "BRANCH":
                    return regexlint._width(node)
            for node in regexlint._nodes(_parser.parse(pattern, 0)):
                for child in regexlint._children(node)[0]:
                    if regexlint._name(child) == "BRANCH":
                        return regexlint._width(child)
            raise AssertionError(f"no BRANCH in {pattern!r}")

        assert branch("(Rechnung|Invoice)") == (8, 9)
        assert branch("(a|aa)") == (1, 2)

    def test_unknown_width_propagates_as_none(self):
        assert regexlint._width((_parser.LITERAL, 97)) == (1, 1)   # sanity
        assert regexlint._width(("NOPE", ())) is None


class TestR1:
    @pytest.mark.parametrize("pattern", [
        "(a+)+$", "(a*)*", "(a?)*b", "^(a*){20}b$", "(a+)+\\d+$", "(x+x+)+y",
        "^(a?){1000}a{1000}$", "^(a?){100}a{100}$", "(a?){10}b",
        "(a{1,3}){40}x", "^(a|ab){40}x", "(a*)*(b*)*$", "(\\s*\\w+\\s*){1,10}rechnung",
        "^(?:\\d{1,3}\\.){3}\\d{1,3}$",     # a "safe-looking" IP filter: R1 refuses it
    ])
    def test_rejects_a_repeated_body_that_is_variable_or_nullable(self, pattern):
        got = analyze(pattern, re.IGNORECASE)
        assert not got.ok, f"{pattern!r} accepted: {got.reason}"
        assert "R1" in got.reason

    @pytest.mark.parametrize("pattern", [
        "^Rechnung[- ]?\\d{3,6}$", "^IN[0-9/\\-]{4,12}$", "^\\d{2,4}[- ]?\\d{3,4}$",
        "^(R|I)N-\\d{6}$", "^(a|b){2}$", "^Q-?\\d{5}$",   # `?` outside a repetition
        "^(?:ab|cd|ef)\\d{4}$", "^FW:\\s*\\w", "^A-\\d{3}(\\.\\d)?$",
    ])
    def test_accepts_fixed_width_bounded_repetitions(self, pattern):
        assert analyze(pattern, re.IGNORECASE).ok, analyze(pattern, re.IGNORECASE).reason
```

Note the IP filter in the reject list: `^(?:\d{1,3}\.){3}\d{1,3}$` is a pattern an operator writes with total confidence, and R1 refuses it because `\d{1,3}` inside a repetition of three is variable-width. It costs an operator their first-choice pattern, so `Analysis.reason` must reach the dashboard verbatim (Task 8, M4) rather than arrive as "invalid filter".

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/bin/pytest -q tests/test_regexlint.py -k "TestWidthModel or TestR1"`
Expected: FAIL — `_sum_widths` missing; the reject assertions report `ok=True`.

- [ ] **Step 3: Add the width model and R1**

```python
# in regexlint.py

def _sum_widths(widths):
    """Sum (min, max) widths; an unbounded member makes the sum unbounded.

    Without this the sum of `(1, None)` raises TypeError, and the fail-closed
    wrapper in analyze() turns that into a rejection: the pattern is refused for
    the right reason by the wrong mechanism, and the rule that should have fired
    stays silent.
    """
    if any(w is None for w in widths):
        return None
    lows = sum(w[0] for w in widths)
    if any(w[1] is None for w in widths):
        return (lows, None)
    return (lows, sum(w[1] for w in widths))


def _width(node, depth: int = 0):
    """(min, max) match width with max None = unbounded; None = UNKNOWN.

    Width is computed PER BRANCH. Summing across alternatives makes `(a|aa)` look
    fixed-width, which is how R1 was once defeated in the analyzer rather than in
    the pattern. An unknown width propagates as None and every caller rejects.
    """
    if depth > MAX_DEPTH:
        return None
    name = _name(node)
    if name in _FIXED_ONE:
        return (1, 1)
    if name in _ZEROWIDTH:
        return (0, 0)
    if name not in _KNOWN_OPS:
        return None
    payload = _payload(node)
    if name == "BRANCH":
        arms = []
        for arm in _seq(payload[1] if len(payload) > 1 else []):
            summed = _sum_widths([_width(c, depth + 1) for c in _nodes(arm)])
            if summed is None:
                return None
            arms.append(summed)
        if not arms:
            return None
        return (min(a[0] for a in arms),
                None if any(a[1] is None for a in arms) else max(a[1] for a in arms))
    if name in _REPEAT_OPS:
        low, high = _repeat_bounds(node)
        body = _sum_widths([_width(c, depth + 1) for c in _children(node)[0]])
        if body is None:
            return None
        body_low, body_high = body
        if high is None or body_high is None:
            return (max(low, 0) * body_low, None)
        return (max(low, 0) * body_low, max(high, 0) * body_high)
    return _sum_widths([_width(c, depth + 1) for c in _children(node)[0]])


def _check_repetition(node) -> None:
    """R1: a repetition that can repeat twice must repeat a fixed-width,
    non-empty-matching body.

    R1 says nothing about how large the bound is: a repetition of 100 fixed
    literals stays legal, a repetition of a 1-2 wide branch does not. `?` and
    `{0,1}`/`{1}` are exempt — a repetition that cannot repeat cannot nest or
    multiply cost, and the exemption is what keeps realistic filters such as
    `^(Rechnung|Invoice)?$` accepted (spec §3).
    """
    low, high = _repeat_bounds(node)
    if not (high is None or high >= 2):
        return
    widths = [_width(c) for c in _children(node)[0]]
    if any(w is None for w in widths):
        raise _Reject("repetition body has no provable width (R1)")
    for width in widths:
        if width[0] == 0 or width[0] != width[1] or width[1] is None:
            raise _Reject("repetition of >=2 must repeat a non-empty-matching "
                          "fixed-width body (R1)")
```

and add the call inside `_walk`, in the repeat branch after the repetition counter:

```python
        if name in _REPEAT_OPS:
            guard.repetitions += 1
            if guard.repetitions > MAX_REPETITIONS:
                raise _Reject(f"pattern exceeds {MAX_REPETITIONS} repetition nodes")
            _check_repetition(item)          # R1 (Task 2), R2 (Task 3)
        _walk(_children(item)[0], depth + 1, guard)
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest -q tests/test_regexlint.py -k "TestWidthModel or TestR1"`
Expected: all pass. `_walk` recursion must call `_check_repetition` for **every** repeat node, including nested ones — `(?=^(a+)+$)` is the witness that the check runs on nodes reached through an assertion.

- [ ] **Step 5: Confirm green, then commit**

Run: `.venv/bin/ruff check . && .venv/bin/pytest -q -m "not integration"`

```bash
git add papra_suite/regexlint.py tests/test_regexlint.py
git commit -m "feat(filter): R1, the fixed-width non-empty repeated body

R1 is the rule that retires (a+)+, (a*)*, (a?)*b, (a{1,3}){40}x and the bounded
combinatorics of (a?){1000}a{1000} by shape rather than timing. It is indifferent
to how large the bound is: 100 fixed literals stay legal, a 1-2 wide branch does
not. `?` and {0,1}/{1} are exempt because a repetition that cannot repeat neither
nests nor multiplies, and that exemption is what keeps ^(Rechnung|Invoice)?$
accepted.

Two bugs found while building it are pinned rather than fixed silently. Summing
(1, None) raised TypeError, and the fail-closed wrapper converted that into a
rejection: right verdict, wrong mechanism, and R1 never fired — hence _sum_widths
and a corpus-wide assertion that no rejection reason is ever 'failed closed'.
Branch width is computed per branch: summing across alternatives makes (a|aa)
look fixed-width, which is the defect that let the historical class through in
the analyzer rather than in the pattern.

An unknown width is a rejection, never a default."
```

---

### Task 3: Rules R2 and R3

**Files:**
- Modify: `papra_suite/regexlint.py`
- Test: `tests/test_regexlint.py`

- [ ] **Step 1: Write the failing tests**

```python
class TestR2AlternationInsideRepetition:
    @pytest.mark.parametrize("pattern", [
        "(a|aa)+b", "(a|aa)+$", "(a|ab)*c", "(a|aa)*b", "^(a|a?){50}b$",
        "(Re: |WG: )+", "(yes|no)*!", "^(foo|bar){2}$",      # documented costs, A.1
        "(?:(a)|a)+x", "(?:(a)|a)+b",                        # the R2-necessity witnesses
        "a(?:xx|(?:b|bb)+c)+d",
    ])
    def test_rejects(self, pattern):
        got = analyze(pattern, re.IGNORECASE)
        assert not got.ok and "R2" in got.reason, f"{pattern!r}: {got.reason}"

    def test_r1_cannot_see_the_r2_witness(self):
        # Independence: (?:(a)|a)+b has single-width branches, so only R2 catches
        # it. If this ever becomes redundant, the redundancy is the bug.
        assert not analyze("(?:(a)|a)+b", re.IGNORECASE).ok
        assert "R2" in analyze("(?:(a)|a)+b", re.IGNORECASE).reason

    @pytest.mark.parametrize("pattern", [
        "^(Rechnung|Invoice|Rechnungsnummer)\\s*\\d{4,10}$",
        "^(?:Invoice|Rechnung)\\s+20\\d{2}$",
        "^(Rechnung|Rechnungen)\\s*#?\\d*$", "^(?!Abmeldung).*Rechnung.*",
        "^(?:Rechnung|Invoice|Re)\\b.*", "\\b(19|20)\\d{2}\\b",
        "^(Rechnung|Invoice)?$", "(a|aa){0,1}b",              # `?`-exempt: at most one
        "(?i)^(re:|wg:)? ?rechnung\\b", "^(R|I)?N\\d{6}", "(R|I)?N\\d{6}",
    ])
    def test_accepts_alternation_outside_a_repetition_or_only_optional(self, pattern):
        assert analyze(pattern, re.IGNORECASE).ok, analyze(pattern, re.IGNORECASE).reason


class TestR3Backreferences:
    @pytest.mark.parametrize("pattern", [
        r"(a)\1+rechnung", r"(a)\1+", r"(ab)\1{2}", r"^(?:(a)|(b))\1$",
        r"^(?:(a)|(b))(?(1)x|y)$",          # GROUPREF_EXISTS: unbounded arms in both
        r"(?=(a))\1b",                      # a backref hidden inside an assertion
    ])
    def test_rejects(self, pattern):
        got = analyze(pattern, re.IGNORECASE)
        assert not got.ok and "backreference" in got.reason, f"{pattern!r}: {got.reason}"


class TestTraversalReachesEverySubpattern:
    def test_lookahead_body_is_checked(self):
        # The spec's traversal witness: (?=^(a+)+$) burns >5 s at 999 characters
        # and is refused only because assertion bodies are descended into. The
        # same hole would hide a backreference or an unknown opcode from R3.
        got = analyze("(?=^(a+)+$)", re.IGNORECASE)
        assert not got.ok and "R1" in got.reason

    def test_look_behind_bodies_are_reached(self):
        # A valid look-behind must be fixed-width, so CPython itself rejects most
        # dangerous shapes inside one; there is no honest rejection witness here.
        # The traversal requirement is proved structurally instead: an ASSERT
        # node yields its body nodes to the walk.
        node = _parser.parse("a(?<=b)c", 0)[1]
        assert regexlint._name(node) == "ASSERT"
        assert [regexlint._name(c) for c in regexlint._children(node)[0]] == ["LITERAL"]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/bin/pytest -q tests/test_regexlint.py -k "TestR2 or TestR3 or TestTraversal"`
Expected: FAIL — the alternation and backreference assertions report `ok=True`.

- [ ] **Step 3: Add R2 and R3**

```python
# in regexlint.py

def _contains_branch(node, depth: int = 0) -> bool:
    """Is there an alternation anywhere inside this node (R2)?"""
    if depth > MAX_DEPTH:
        return True                      # too deep to prove: refuse
    if _name(node) == "BRANCH":
        return True
    return any(_contains_branch(c, depth + 1) for c in _children(node)[0])
```

extend `_check_repetition` inside the `high is None or high >= 2` guard, after the R1 loop:

```python
    for child in _children(node)[0]:
        if _contains_branch(child):
            raise _Reject("alternation inside a repetition (R2)")
```

and add R3 in **two** places. First, at the top of the per-item block in `_walk`, before the unknown-opcode check:

```python
        if name in ("GROUPREF", "REF", "GROUPREF_EXISTS"):
            raise _Reject("backreferences are refused (R3)")
```

Second, at the top of `_width`, right after `name = _name(node)`:

```python
    if name in ("GROUPREF", "REF", "GROUPREF_EXISTS"):
        raise _Reject("backreferences are refused (R3)")
```

The second one is not belt-and-braces. `(a)\1+` reaches `_check_repetition` before the walk ever descends to the `GROUPREF` node, and `GROUPREF`'s children are empty, so its width comes out `(0, 0)` and the pattern is refused as "empty-matching body (R1)" — refused for a reason it has nothing to do with. A backreference is precisely the construct whose width cannot be derived, so it must refuse **at the point the width is asked for**; without this, six of the corpus's backreference fixtures report R1 and the reason the operator sees is a lie. This is the same failure mode as the `TypeError` in Task 2, and Task 9's "no rejection may come from an accident" test is what keeps it from creeping back.

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest -q tests/test_regexlint.py -k "TestR2 or TestR3 or TestTraversal"`
Expected: all pass, and every backreference fixture's reason says `backreference` — if any says `R1`, the `_width` insertion is missing. Then run the whole file: the A.2 accepts must stay accepted, which is what the `?`-exemption in `_check_repetition` protects — moving the R2 loop outside that guard rejects five realistic optional-alternation filters.

- [ ] **Step 5: Confirm green, then commit**

Run: `.venv/bin/ruff check . && .venv/bin/pytest -q -m "not integration"`

```bash
git add papra_suite/regexlint.py tests/test_regexlint.py
git commit -m "feat(filter): R2, R3, and the guard placement that keeps `?` legal

R2 is independent of R1 and both witnesses are pinned: (?:(a)|a)+b has
single-width branches so R1 cannot see its exponential ambiguity, and (a|aa)+b
is the shape that shipped in the README until M2. R3 refuses backreferences
because their width is not statically derivable — (a)\\1+ desugars into a
repetition whose body R1 cannot evaluate — and GROUPREF_EXISTS can hide
unbounded arms behind a condition, in both directions.

R2 lives INSIDE the repetition guard with R1, not beside it: an alternation
under `?` cannot repeat, and applying R2 unconditionally rejects
^(Rechnung|Invoice)?$ and four other realistic filters. That placement mistake
was made while drafting this and is why the exemption has its own tests.

There is no honest rejection witness inside a look-behind: CPython requires a
fixed-width look-behind, so the dangerous shapes never parse. The traversal
requirement there is pinned structurally instead, which says what is actually
true rather than dressing up a rejection."
```

---

### Task 4: Rule R4 — overlapping adjacent repetitions

**Files:**
- Modify: `papra_suite/regexlint.py`
- Test: `tests/test_regexlint.py`

**The measured basis (1 000-character subject, the cap; each run in its own process
with a 10 s timeout):**

| empty-matching unbounded repetitions | measured | R4 |
| --- | --- | --- |
| 2 (`^a*a*b$`) | 0.000 s | accepted |
| 3 (`^a*a*a*b$`) | 0.072 s | **rejected** |
| 4 (`^a*a*a*a*b$`) | **> 10 s** | rejected |
| 3 unbounded + a bounded empty-matching member (`^a*a*#?a*a*a*x$`) | **> 10 s** | rejected |

The last row is why the run counts unbounded **members** rather than resetting on a
bounded item: an empty-matching `#?` between them does not break the overlap. The
threshold is 3 against a measured cliff at 4, which is a declared policy margin, not
a measurement. `^a*a*#?a*$` measures 0.000 s and is still refused — same shape, and
its catastrophic sibling needs only a tail that fails to match.

- [ ] **Step 1: Write the failing tests**

```python
class TestR4AdjacentOverlappingRepetitions:
    @pytest.mark.parametrize("pattern", [
        "^a*a*a*b$", "^a*a*a*a*b$", "^a*a*a*a*a*a*a*a*a*a*a*a*b$",
        "^\\s*\\s*\\s*x$", "^\\s*a*\\s*\\s*x$", "^\\s*a*a*\\s*a*\\s*x$",
        "^a*a*#?a*$",            # cheap (0.000 s) and refused: the shape is the rule
        "^a*a*#?a*a*a*x$",       # the bounded member does not break the run
    ])
    def test_rejects(self, pattern):
        got = analyze(pattern, re.IGNORECASE)
        assert not got.ok and "R4" in got.reason, f"{pattern!r}: {got.reason}"

    @pytest.mark.parametrize("pattern", [
        "^a*a*b$",              # exactly two: 0.000 s at the cap
        "^a*b$", "^\\w*\\w*$",
        "^\\s*#?\\d{1,10}$", "^\\s*\\s*#?\\d{1,10}$",
        "^\\s*(Rechnung|Invoice)\\s+\\d+\\s*$",
        "^\\d{2}\\s*\\s*x$",    # a non-nullable item breaks the run
    ])
    def test_accepts(self, pattern):
        assert analyze(pattern, re.IGNORECASE).ok, analyze(pattern, re.IGNORECASE).reason

    def test_the_ordering_the_threshold_was_chosen_on(self):
        # Measured at 80 characters so the suite stays fast; the same ordering
        # holds at the cap, where 4 unbounded repetitions exceed 10 s.
        def cost(pattern: str) -> float:
            subject = "a" * 80
            start = time.perf_counter()
            re.search(pattern, subject)          # no trailing b: the match must FAIL
            return time.perf_counter() - start

        two, three, four = cost("^a*a*b$"), cost("^a*a*a*b$"), cost("^a*a*a*a*b$")
        assert three > two * 1.5
        assert four > two * 10

    def test_r4_does_not_double_as_a_complexity_bound(self):
        # R4 counts a syntactic run; it is not a budget. This stays accepted and
        # is cheap at the cap, which is the honest face of the policy.
        assert analyze("(a|b)+x", re.IGNORECASE).ok
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/bin/pytest -q tests/test_regexlint.py -k TestR4`
Expected: FAIL — every rejection assertion reports `ok=True`.

- [ ] **Step 3: Add R4**

```python
# in regexlint.py

def _unbounded_in_run(items) -> int:
    """Longest run of empty-matching items holding UNBOUNDED repetitions (R4).

    Every item in the run can match empty and every counted repetition is
    unbounded, so a tail that fails to match multiplies split points across k
    positions: 2 is free, 3 costs 0.072 s at the cap, 4 is >10 s. A bounded
    empty-matching member (`#?`) sits in the run without advancing it, because it
    adds its own split points without restoring a fixed-width anchor.
    """
    run = worst = 0
    for item in items:
        if not _is_node(item):
            run = 0
            continue
        width = _width(item)
        if _name(item) in _REPEAT_OPS and width is not None and width[0] == 0:
            if width[1] is None:
                run += 1
                worst = max(worst, run)
            # empty-matching but bounded: the run continues, uncounted
        else:
            run = 0                     # fixed-width or non-nullable: run broken
    return worst
```

and at the top of `_walk`, before the item loop:

```python
    if _unbounded_in_run(list(items)) >= 3:
        raise _Reject("three or more adjacent empty-matching unbounded repetitions "
                      "(R4: combinatorial overlap)")
```

Running it per `_walk` call is what makes it forest-wide: the top-level sequence is
one call and every repetition body, group body, branch arm and assertion body is
another.

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest -q tests/test_regexlint.py -k TestR4`
Expected: all pass. If `^\\s*\\s*x$`-shaped accepts fail, `_unbounded_in_run` is counting bounded repetitions; if `^a*a*#?a*$` is accepted, the counter is resetting on the `#?`.

- [ ] **Step 5: Re-verify the corpora of record against the finished rules**

Run: `.venv/bin/python scripts/filter_contract_check.py --corpora` (the script arrives in Task 9; until then, run the appendix fixtures from the spec through `analyze` in one script)
Expected: 29 must-reject patterns all rejected **with a rule or bound named in the reason** (never "failed closed"), 39 must-accept patterns all accepted. These are the numbers this plan's drafting measured on CPython 3.12.15; if they move, the rules changed and the corpora must be re-derived, not patched.

- [ ] **Step 6: Confirm green, then commit**

Run: `.venv/bin/ruff check . && .venv/bin/pytest -q -m "not integration"`

```bash
git add papra_suite/regexlint.py tests/test_regexlint.py
git commit -m "feat(filter): R4, overlapping adjacent repetitions

^a*a*a*a*b\$ is a single literal body, fixed-width, non-nullable, with no
alternation anywhere, inside every size bound — and it exceeds 10 seconds at
exactly the 1000-character subject cap. R1-R3 cannot see it, so the claim that
the cap keeps accepted patterns cheap was false and residual 1 was leaning on it.

The rule counts unbounded repetitions inside a run of items that can all match
empty, measured at the cap: 2 is 0.000 s, 3 is 0.072 s, 4 is >10 s, and 3 with a
bounded empty-matching member between them is also >10 s — which is why a bounded
member does not reset the run. Threshold 3 against a cliff at 4 is a stated
margin, and the timing test measures the ordering at 80 characters so the suite
stays fast rather than asserting the cap-length numbers.

^a*a*#?a*$ is refused at 0.000 s and the test says so out loud: the shape is the
rule, not the measured cost of this one instance. An R4 rejection that an
operator cannot afford is a policy question for §11, not an exception to carve
out here."
```

---

### Task 5: The `analyze` op in the killable child

**Files:**
- Modify: `papra_suite/regex_worker.py`
- Modify: `papra_suite/safematch.py`
- Test: `tests/test_safematch.py`

**Interfaces:**
- Consumes: `regexlint.analyze`, `regexlint.ANALYSIS_POLICY_VERSION`, `KillableMatcher._request`.
- Produces:

```python
# protocol (regex_worker.py docstring updated to match)
in:  {"id": int, "op": "search"|"validate"|"analyze", ...}
out: {"id": int, "matched": bool}                          search
     {"id": int, "ok": true, "policy": int}                analysis/validate accepted
     {"id": int, "error": str, "policy": int}              rejected (reason for the UI)
     {"id": int, "analysis": "timeout", "policy": int}     the ANALYSIS deadline blew

class KillableMatcher:
    async def analyze(self, pattern: str, *, timeout_s: float) -> Analysis
        # returns Analysis(ok, reason); raises MatcherUnavailable when the child
        # cannot answer, or when the analysis deadline blew twice in a row
```

- [ ] **Step 1: Write the failing tests**

```python
# appended to tests/test_safematch.py
class TestAnalyzeOp:
    async def test_analyze_accepts_a_safe_pattern_and_reports_the_policy_version(self):
        matcher = KillableMatcher()
        try:
            got = await matcher.analyze("^Rechnung\\s*#?\\d{4,10}$", timeout_s=2.0)
            assert got.ok and got.reason == ""
        finally:
            await matcher.aclose()

    async def test_analyze_rejects_with_the_reason_that_surfaces_in_the_ui(self):
        matcher = KillableMatcher()
        try:
            got = await matcher.analyze("(a+)+$", timeout_s=2.0)
            assert not got.ok and "R1" in got.reason
        finally:
            await matcher.aclose()

    async def test_analysis_timeout_frame_is_retried_once_then_fails_closed(self, monkeypatch):
        # ANALYSIS_TIMEOUT must never travel as a bare EOF: a missing child reads
        # as "infrastructure is broken" and the wrong response to that is to
        # respawn and burn the budget again on the same pattern.
        frames = iter([{"analysis": "timeout"}, {"analysis": "timeout"}])

        async def fake_request(payload, *, timeout_s):
            return next(frames)

        matcher = KillableMatcher()
        monkeypatch.setattr(matcher, "_request", fake_request)
        with pytest.raises(MatcherUnavailable):
            await matcher.analyze("^a*$", timeout_s=0.01)

    async def test_analysis_timeout_that_clears_on_retry_is_not_a_failure(self, monkeypatch):
        frames = iter([{"analysis": "timeout"}, {"ok": True, "policy": 1}])

        async def fake_request(payload, *, timeout_s):
            return next(frames)

        matcher = KillableMatcher()
        monkeypatch.setattr(matcher, "_request", fake_request)
        assert (await matcher.analyze("^a*$", timeout_s=0.01)).ok

    async def test_a_child_killed_by_its_own_alarm_reads_as_a_timeout_not_an_eof(self):
        # The classification matters: an EOF that means "this child is broken"
        # respawns and re-burns the budget; a self-deadline death is THIS
        # pattern's fault and must be terminal.
        assert safematch._died_at_self_deadline(type("P", (), {"returncode": -signal.SIGALRM})())
        assert not safematch._died_at_self_deadline(type("P", (), {"returncode": 1})())
        assert not safematch._died_at_self_deadline(type("P", (), {"returncode": None})())
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/bin/pytest -q tests/test_safematch.py -k TestAnalyzeOp`
Expected: FAIL — `AttributeError: 'KillableMatcher' object has no attribute 'analyze'`.

- [ ] **Step 3: Add the op to the child and the method to the parent**

```python
# papra_suite/regex_worker.py — the analysis deadline the CHILD gives the analyzer,
# kept below the SIGALRM bound so the analyzer can answer instead of dying. Without
# a frame the parent would see an EOF and read "this child is broken", which is the
# one reading that makes it respawn and burn the budget again (spec §5.2, §9).
ANALYSIS_MARGIN_S = 0.25


def _analysis_deadline_s(request: dict) -> float:
    from papra_suite.regexlint import ANALYSIS_MIN_S, ANALYSIS_TIMEOUT_FALLBACK_S

    budget = _self_deadline_s(request)
    if budget - ANALYSIS_MARGIN_S < ANALYSIS_MIN_S:
        return ANALYSIS_TIMEOUT_FALLBACK_S      # tiny request: still give it a real clock
    return budget - ANALYSIS_MARGIN_S
```

```python
# papra_suite/regex_worker.py, in _handle, beside the existing validate arm
        if operation == "analyze":
            # imported lazily: the search path must not pay for the analyzer's
            # import chain, and the analyzer must be able to run here at all
            import re as _re

            from papra_suite import regexlint

            try:
                analysis = regexlint.analyze(pattern, _re.IGNORECASE,
                                             deadline_s=_analysis_deadline_s(request))
            except regexlint.AnalysisTimeout:
                return {"id": request_id, "analysis": "timeout",
                        "policy": regexlint.ANALYSIS_POLICY_VERSION}
            if analysis.ok:
                return {"id": request_id, "ok": True,
                        "policy": regexlint.ANALYSIS_POLICY_VERSION}
            return {"id": request_id, "error": analysis.reason,
                    "policy": regexlint.ANALYSIS_POLICY_VERSION}
        if operation == "validate":
            # Task 8 points this arm at the SAME admission function as `analyze`;
            # until then it is the legacy ratio gate and the two ops are NOT
            # required to agree — that agreement is Task 8's test, not this one.
            from papra_suite.mail import validate_subject_filter

            validate_subject_filter(pattern)
            return {"id": request_id, "ok": True}
```

`re.IGNORECASE` is spelled literally here because `mail.SUBJECT_FILTER_FLAGS` does not exist until Task 8; Task 8 Step 4 replaces this literal with that constant, and that is the only edit this task's code receives later.

```python
# papra_suite/safematch.py
import signal

from papra_suite.regexlint import Analysis


def _died_at_self_deadline(process) -> bool:
    """True when the child was killed by its own SIGALRM self-deadline."""
    if not hasattr(signal, "SIGALRM"):
        return False
    return process.returncode == -signal.SIGALRM
```

```python
# papra_suite/safematch.py, method on KillableMatcher
    async def analyze(self, pattern: str, *, timeout_s: float) -> Analysis:
        """Static analysis in the child: the rules are CPU work too.

        A pathological pattern can make `re._parser.parse` itself slow, so the
        analyzer runs where everything else dangerous runs and is killable. Two
        ways it can run out of time and they arrive differently — the analyzer
        self-aborts and answers `{"analysis": "timeout"}`, or the alarm kills the
        child and the parent sees a death. Both are retried ONCE and then become
        MatcherUnavailable: an unknown pattern is never admitted, and "the
        analyzer was slow" is never reported as "your pattern is fine".
        """
        if not pattern:
            return Analysis(True)
        for attempt in (1, 2):
            try:
                reply = await self._request(
                    {"op": "analyze", "pattern": pattern, "subject": "", "flags": 0},
                    timeout_s=timeout_s,
                )
            except MatchTimeout as error:
                if attempt == 2:
                    raise MatcherUnavailable(
                        f"subject filter analysis exceeded {timeout_s}s twice"
                    ) from error
                logger.warning("analysis ran out of time, retrying once: %s", error)
                continue
            if reply.get("analysis") == "timeout":
                if attempt == 2:
                    raise MatcherUnavailable(
                        f"the subject filter analysis hit its deadline twice: {pattern!r}"
                    )
                logger.warning("analysis deadline frame, retrying once: %r", pattern)
                continue
            if "error" in reply:
                return Analysis(False, str(reply["error"]))
            return Analysis(True)
        return Analysis(False, "analysis did not answer")   # unreachable, defensive
```

`papra_suite/regexlint.py` gains the two constants the worker needs:

```python
ANALYSIS_MIN_S = 0.05                # below this the analyzer cannot finish a parse anyway
ANALYSIS_TIMEOUT_FALLBACK_S = 0.25   # a request too small to carry a real clock
```

- [ ] **Step 4: Run the tests**

Run: `.venv/bin/pytest -q tests/test_safematch.py -k TestAnalyzeOp`
Expected: all pass. The two frame tests are the load-bearing ones: they pin that a slow analysis is *answered* rather than *survived*, which is the distinction between a retry and an infrastructure alarm.

- [ ] **Step 5: Confirm green, then commit**

Run: `.venv/bin/ruff check . && .venv/bin/pytest -q -m "not integration"`

```bash
git add papra_suite/regex_worker.py papra_suite/safematch.py tests/test_safematch.py
git commit -m "feat(filter): run the analyzer inside the killable child, with its own frame

The static rules are CPU work over operator-chosen input — re._parser.parse can
itself be driven slowly — so `analyze` goes through the same transport that
already makes the search and the validator killable, and validate/analyze share
one child-side function so a pattern cannot be admissible at save time and
refused at poll time.

ANALYSIS_TIMEOUT is an explicit reply rather than an absent one: a missing frame
would be read as a dead child, which is the one response that makes the parent
respawn and burn the budget again. A self-deadline death is classified from the
child's return code, so THIS pattern being slow no longer looks like infrastructure
being broken. Analysis timeout is retried once and then fails closed: an unknown
pattern is never admitted."
```

---

### Task 6: One deadline per message, shared across the retries

**Files:**
- Modify: `papra_suite/safematch.py`
- Test: `tests/test_safematch.py`

**Problem (measured, spec §11.2):** `_request` retries EOF/`OSError`/malformed deaths, and each attempt can burn a full `timeout_s` before dying at the child's own alarm. Three attempts on a hostile subject is ~6 s of child CPU per message — the residual that the shared message-wide deadline exists to close.

**Interfaces:**
- Produces:

```python
_MIN_ATTEMPT_S = 0.05                     # below this, a new attempt cannot finish setup

def _attempt_budget(deadline: float, now: float) -> float
    # remaining seconds until `deadline`; <=0 when there is nothing left

# KillableMatcher._request(payload, *, timeout_s) keeps its signature but the
# deadline is now computed ONCE for the call and shared by every attempt; the
# child's self-deadline is derived from the remaining budget.
```

- [ ] **Step 1: Write the failing tests**

```python
class TestSharedMessageDeadline:
    def test_attempt_budget_shrinks_and_empties(self):
        end = 100.0
        assert safematch._attempt_budget(end, 98.0) == pytest.approx(2.0)
        assert safematch._attempt_budget(end, 100.0) <= 0
        assert safematch._attempt_budget(end, 101.0) <= 0

    async def test_a_child_that_dies_at_its_alarm_is_not_retried(self, monkeypatch):
        # Pre-fix: alarm death arrives as EOFError, is retried, and burns a second
        # full budget on the same hostile subject.
        sent: list[int] = []

        matcher = KillableMatcher()
        calls = {"n": 0}

        async def fake_readline():
            calls["n"] += 1
            sent.append(1)
            raise EOFError                      # what a SIGALRM death looks like on the wire

        async def fake_ensure():
            return type("P", (), {
                "returncode": -signal.SIGALRM,
                "stdin": type("S", (), {"write": lambda *_: None,
                                        "drain": lambda: asyncio.sleep(0)})(),
                "stdout": type("O", (), {"readline": fake_readline})(),
                "pid": 4242, "kill": lambda *_: None, "terminate": lambda *_: None,
                "wait": lambda: asyncio.sleep(0),
            })()

        monkeypatch.setattr(matcher, "_ensure_process", fake_ensure)
        monkeypatch.setattr(matcher, "_retire", lambda *a: asyncio.sleep(0))
        with pytest.raises(MatchTimeout):
            await matcher._request({"op": "search", "pattern": "a", "subject": "b",
                                    "flags": 0}, timeout_s=1.0)
        assert len(sent) == 1                   # terminal, not retried twice more

    async def test_total_wall_time_stays_inside_one_budget(self):
        # The end-to-end promise: 2.0 s per message, not 2.0 s per attempt.
        matcher = KillableMatcher()
        try:
            start = time.perf_counter()
            outcome = await matcher.search("(a+)+$", "a" * 40,
                                          timeout_s=safematch.SUBJECT_FILTER_TIMEOUT_S
                                          if hasattr(safematch, "SUBJECT_FILTER_TIMEOUT_S") else 2.0)
            elapsed = time.perf_counter() - start
            assert outcome is MatchOutcome.TIMEOUT
            assert elapsed < 2.0 + 0.6, f"spent {elapsed:.2f}s on one subject"
        finally:
            await matcher.aclose()
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/bin/pytest -q tests/test_safematch.py -k TestSharedMessageDeadline`
Expected: FAIL — `_attempt_budget` missing; the alarm-death case raises `MatcherUnavailable` after three attempts; the wall-time test exceeds the bound.

- [ ] **Step 3: Implement the shared deadline**

```python
# papra_suite/safematch.py
import signal

_MIN_ATTEMPT_S = 0.05


def _attempt_budget(deadline: float, now: float) -> float:
    """Seconds left for one attempt inside the message-wide deadline."""
    return deadline - now


    async def _request(self, payload: dict, *, timeout_s: float) -> dict:
        """One logical operation: ONE deadline, shared by every attempt.

        `timeout_s` is a budget for the whole operation, not per attempt. A
        deadline-burning death (EOF after the child's own alarm, or a kill) must
        not buy another full budget: three attempts at 2 s each is 6 s of child
        CPU on one subject, which was the worst case the per-attempt reading
        left in place (spec §11.2).
        """
        async with self._lock:
            loop = asyncio.get_running_loop()
            end = loop.time() + timeout_s
            attempts = 0
            while True:
                attempts += 1
                remaining = _attempt_budget(end, loop.time())
                if remaining <= _MIN_ATTEMPT_S:
                    raise MatcherUnavailable(
                        f"the {timeout_s}s budget for this message was spent on "
                        f"{attempts - 1} attempt(s); no further retry"
                    )
                process = await self._ensure_process()
                self._next_id += 1
                request_id = self._next_id
                request = {"id": request_id,
                           "deadline_s": _self_deadline_s(remaining),
                           **payload}
                line = json.dumps(request).encode("utf-8") + b"\n"
                try:
                    process.stdin.write(line)
                    await process.stdin.drain()
                except (OSError, RuntimeError) as error:
                    await self._retire(process, f"request could not be sent: {error}")
                    if attempts > MAX_CHILD_RETRIES:
                        raise MatcherUnavailable(
                            f"could not send a request to the regex child: {error}"
                        ) from error
                    continue

                try:
                    reply_line = await asyncio.wait_for(
                        process.stdout.readline(), remaining
                    )
                except TimeoutError as error:
                    await self._retire(process, f"no reply within {remaining:.3f}s")
                    raise MatchTimeout(
                        f"the regex child exceeded its {timeout_s}s budget and was killed"
                    ) from error
                except (EOFError, OSError, ValueError) as error:
                    await self._retire(process, f"reading the reply failed: {error}")
                    if _died_at_self_deadline(process):
                        # THIS pattern overran: retrying buys another burn, not an answer
                        raise MatchTimeout(
                            f"the regex child hit its own {remaining:.3f}s deadline"
                        ) from error
                    if attempts > MAX_CHILD_RETRIES:
                        raise MatcherUnavailable(
                            f"the regex child died while answering: {error}"
                        ) from error
                    continue

                reply, problem = _parse_reply(reply_line, request_id)
                if reply is not None:
                    return reply
                await self._retire(process, problem)
                if attempts > MAX_CHILD_RETRIES:
                    raise MatcherUnavailable(problem)
```

Keep `_self_deadline_s(remaining)` — the child's alarm still sits *above* the parent's remaining budget, so the parent's kill wins in the normal case.

- [ ] **Step 4: Run the tests, including the whole safematch file**

Run: `.venv/bin/pytest -q tests/test_safematch.py`
Expected: all pass, including every M3 test about retries. If a Wave-D test that expected `MatcherUnavailable` after repeated deaths now sees `MatchTimeout`, check whether that child died by SIGALRM (`-14`) — only that death changes classification; a crash with status 1 must still retry.

- [ ] **Step 5: Confirm green, then commit**

Run: `.venv/bin/ruff check . && .venv/bin/pytest -q -m "not integration"`

```bash
git add papra_suite/safematch.py tests/test_safematch.py
git commit -m "fix(filter): one regex deadline per message, not one per attempt

_request retried deadline-burning deaths, and each attempt could spend a full
budget before dying at the child's own alarm: three attempts is ~6 s of child
CPU on a single hostile subject, against a contract that reads as 2 s. The
deadline is now computed once for the operation, attempts get only what remains,
and a child that died at its own SIGALRM is terminal instead of a respawn cue —
the difference between 'this pattern is slow' and 'this child is broken'.

The bound is now worst-case per message, which is the number the poller's
shutdown and interval maths actually depend on."
```

---

### Task 7: The two caps and the `SUBJECT_TOO_LONG` disposition

**Files:**
- Modify: `papra_suite/mail.py` (`MAX_RAW_SUBJECT_HEADER_CHARS`, `ParsedMessage`, `parse_message`, `subject_matches`, `PollResult`, the poll loop)
- Modify: `papra_suite/store.py` (`Mailbox` field, `_row_to_mailbox`, migration 002, `record_subject_too_long`)
- Test: `tests/test_mail_parsing.py`, `tests/test_mail_poller.py`, `tests/test_store.py`

**Interfaces:**
- Consumes: nothing new.
- Produces:

```python
MAX_RAW_SUBJECT_HEADER_CHARS = 32_768     # work cap on the undecoded header

class SubjectTooLong(Exception):
    """A subject is over the cap, so the filter cannot answer either way."""

@dataclass ParsedMessage:  subject_too_long: bool = False      # appended last
@dataclass PollResult:     subjects_too_long: int = 0          # appended last
@dataclass Mailbox:        subject_too_long_count: int = 0     # appended last

Store.record_subject_too_long(mailbox_id: str, *, delta: int = 1) -> int
```

- [ ] **Step 1: Write the failing tests — the truncation bug, both directions**

```python
# tests/test_mail_parsing.py
class TestSubjectLengthContract:
    def test_no_filter_keeps_the_whole_subject(self):
        raw = b"Subject: " + b"x" * 1200 + b"\r\n\r\n"
        parsed = mail.parse_message(raw)
        assert len(parsed.subject) == 1200
        assert not parsed.subject_too_long          # no filter is involved

    def test_a_filtered_subject_over_the_cap_is_too_long_not_truncated(self):
        raw = b"Subject: " + b"x" * 1001 + b"\r\n\r\n"
        parsed = mail.parse_message(raw)
        assert len(parsed.subject) == 1001
        assert parsed.subject_too_long

    def test_raw_header_cap_uses_the_undecoded_length(self):
        # Encoded-word filler is free at the wire level and expensive to decode:
        # the cap bounds the DECODER'S work, so it reads the raw length.
        filler = " " * 40
        raw = ("Subject: =?utf-8?B?" + filler + "?=\r\n\r\n").encode()
        parsed = mail.parse_message(raw)
        assert parsed.subject_too_long is (len(filler) + 20 > mail.MAX_RAW_SUBJECT_HEADER_CHARS)

    def test_a_second_subject_header_is_invisible_to_the_filter(self):
        # Documented and pinned, not fixed (spec §11.10): message.get() reads the
        # first header, so a second one is neither shown nor filtered.
        raw = (b"Subject: Rechnung 1\r\nSubject: secret-invoice\r\n\r\n")
        parsed = mail.parse_message(raw)
        assert parsed.subject == "Rechnung 1"

    def test_sync_subject_matches_refuses_instead_of_answering_wrong(self):
        # The shipped defect, both directions, retired:
        #   1050 chars -> matched a pattern the real subject fails
        #   1109 chars -> missed a pattern the real subject satisfies
        pattern = r"^Rechnung.*\d{3}$"
        too_long = "Rechnung" + "x" * 1042 + "12"        # 1050 chars, no 3-digit run
        with pytest.raises(mail.SubjectTooLong):
            mail.subject_matches(too_long, pattern)
        with pytest.raises(mail.SubjectTooLong):
            mail.subject_matches("Rechnung" + "1" * 1103, pattern)

    def test_at_the_cap_still_answers_normally(self):
        assert mail.subject_matches("R" * 991 + "123", r"^R.*\d{3}$")
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/bin/pytest -q tests/test_mail_parsing.py -k TestSubjectLengthContract`
Expected: FAIL — the 1050-char case currently returns `True` (the false positive), proving the bug; `subject_too_long` does not exist.

- [ ] **Step 3: Implement the parse side and the sync helper**

```python
# papra_suite/mail.py, next to MAX_SUBJECT_CHARS
MAX_RAW_SUBJECT_HEADER_CHARS = 32_768   # work cap on the UNDECODED header (spec §4)


class SubjectTooLong(Exception):
    """The subject is over the cap, so a configured filter cannot answer.

    Not a match and not a non-match: the pattern's answer is simply undefined
    for input it is not allowed to see (spec §4.1).
    """


@dataclass
class ParsedMessage:
    message_id: str | None
    subject: str | None
    pdf_attachments: list[Attachment] = field(default_factory=list)
    skipped_attachments: list[str] = field(default_factory=list)
    # appended last so the pre-existing positional order survives
    subject_too_long: bool = False


def _raw_header_length(value: str | bytes | None) -> int:
    """Length of the header as it sits on the wire, before any decoding."""
    if value is None:
        return 0
    if isinstance(value, bytes):
        # lossy and cheap: we only need the length, and a strict decode would
        # raise on the very malformed input this cap exists to survive
        return len(value.decode("raw-unicode-escape", "ignore"))
    return len(value)
```

In `parse_message`, replace `subject = _decode_header_value(msg.get("Subject")) if msg.get("Subject") else None` with:

```python
    raw_subject = msg.get("Subject")           # FIRST header only, by design (§11.10)
    subject_too_long = _raw_header_length(raw_subject) > MAX_RAW_SUBJECT_HEADER_CHARS
    subject = _decode_header_value(raw_subject) if raw_subject else None
```

and pass `subject_too_long=subject_too_long` to the returned `ParsedMessage`. In `subject_matches`, replace the truncation with the refusal:

```python
    if len(subject) > MAX_SUBJECT_CHARS:
        raise SubjectTooLong(
            f"subject is {len(subject)} characters, over the {MAX_SUBJECT_CHARS}-character "
            "filter cap: the filter cannot answer for it (neither match nor non-match)"
        )
    return re.search(pattern, subject, re.IGNORECASE) is not None
```

Update `subject_matches`' docstring: the truncation paragraph is replaced by "over the cap raises `SubjectTooLong`, because truncating produced wrong answers in BOTH directions — measured `^Rechnung.*\\d{3}$` matching at 1050 characters and missing at 1109."

- [ ] **Step 4: Add the store column, the counter, and the poll disposition**

```python
# papra_suite/store.py
def _migration_002_subject_too_long(conn: sqlite3.Connection) -> None:
    conn.execute(
        "ALTER TABLE mailboxes ADD COLUMN subject_too_long_count INTEGER NOT NULL DEFAULT 0"
    )


MIGRATIONS = [_migration_001_initial, _migration_002_subject_too_long]


    def record_subject_too_long(self, mailbox_id: str, *, delta: int = 1) -> int:
        """Count retired over-long subjects. Approximate BY CONTRACT (spec §8.2):
        mark_seen and this write are separate steps, fetch_since does not filter
        \\Seen, and a UIDVALIDITY change re-fetches — so a crash between the two
        re-counts. The counter exists so a mailbox is not quietly missing mail."""
        self._conn.execute(
            "UPDATE mailboxes SET subject_too_long_count = subject_too_long_count + ? "
            "WHERE id = ?",
            (delta, mailbox_id),
        )
        self._conn.commit()
        row = self._conn.execute(
            "SELECT subject_too_long_count FROM mailboxes WHERE id = ?", (mailbox_id,)
        ).fetchone()
        if row is None:
            raise ValueError(f"unknown mailbox: {mailbox_id}")
        return int(row[0])
```

Add `subject_too_long_count: int = 0` to the `Mailbox` dataclass (it lives in `store.py`, last position) and map it in `_row_to_mailbox`.

In the poll loop, immediately after `parsed = await asyncio.to_thread(parse_message, raw)` and before `_subject_filter_match`:

```python
            # An over-long subject is retired, NOT matched against a truncated
            # copy and NOT reported as a non-match: with a filter configured the
            # pattern's answer is undefined, and calling it NOMATCH would hide a
            # mailbox that is missing every invoice it was configured to catch.
            # It is terminal for this message — one terminal disposition per
            # pass — and it never counts as a poll failure, because nothing about
            # the mailbox or the pattern is broken (spec §8).
            if mailbox.subject_filter and (
                parsed.subject_too_long or len(parsed.subject or "") > mail.MAX_SUBJECT_CHARS
            ):
                result.subjects_too_long += 1
                _record_subject_too_long_quietly(store, mailbox.id)
                logger.warning(
                    "message uid=%s has a subject over the %d-character filter cap; "
                    "it was skipped and marked seen, and the counter for this "
                    "mailbox is approximate",
                    uid, MAX_SUBJECT_CHARS,
                )
                seen_uids.append(uid)
                if not prefix_failed:
                    candidate_last_seen = uid
                continue
```

with the sibling of the existing quiet recorders:

```python
def _record_subject_too_long_quietly(store: Store, mailbox_id: str) -> None:
    try:
        store.record_subject_too_long(mailbox_id)
    except (ValueError, sqlite3.Error) as error:
        logger.error("failed to count a too-long subject for mailbox %s: %s",
                     mailbox_id, error)
```

and `subjects_too_long: int = 0` on `PollResult` (appended last). In `_subject_filter_match`, drop the silent truncation — `subject[:MAX_SUBJECT_CHARS]` becomes `subject`, since the length is now a precondition the caller has already checked.

- [ ] **Step 5: Write the poll-level tests**

```python
# tests/test_mail_poller.py
def _persisted(store, *, subject_filter=".*rechnung.*"):
    """A mailbox row in the DB, because the counter and the error state live there."""
    org_id = store.create_org(papra_org_id="org_papra", name="Test")
    mailbox_id = store.create_mailbox(
        org_id=org_id, host="imap.test", port=993, username="u", password="p",
        subject_filter=subject_filter,
    )
    return store.get_org(org_id), store.get_mailbox(mailbox_id), mailbox_id


async def test_subject_too_long_is_retired_counted_and_never_matched(store, monkeypatch):
    org, mailbox, mailbox_id = _persisted(store)
    long_subject = "x" * 1001 + " rechnung"
    session = FakeSession(raws=[(1, build_raw(subject=long_subject, message_id="<m1@x>",
                                                 attachments=[("a.pdf", b"%PDF-1")]))])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    result = await poll_mailbox_once(store, client, mailbox, org)

    assert result.subjects_too_long == 1
    assert result.documents_uploaded == 0 and client.uploads == []
    assert store.claim_next_job("ocr") is None            # no job for a retired message
    assert session.seen == [1]                            # retired, not re-armed
    assert store.get_mailbox(mailbox_id).last_seen_uid == 1
    assert store.get_mailbox(mailbox_id).subject_too_long_count == 1
    assert store.get_mailbox(mailbox_id).last_error is None
    assert store.get_mailbox(mailbox_id).enabled is True


async def test_three_overlong_subjects_never_disable_the_mailbox(store, monkeypatch):
    # The disable path must not be reachable from a sender's choice of subject:
    # 3-strike disablement here would be a remote kill switch for a mailbox.
    org, mailbox, mailbox_id = _persisted(store)
    session = FakeSession(raws=[
        (uid, build_raw(subject="x" * 1200, message_id=f"<m{uid}@x>")) for uid in (1, 2, 3)
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    for _ in range(3):
        result = await poll_mailbox_once(store, StubClient(), store.get_mailbox(mailbox_id), org)
        assert result.error is None

    after = store.get_mailbox(mailbox_id)
    assert after.enabled is True and after.consecutive_failures == 0
    assert after.subject_too_long_count == 3
    assert result.subjects_too_long == 0        # third poll saw nothing new


async def test_no_filter_ingests_an_overlong_subject_whole(store, monkeypatch):
    # The cap exists to bound the EVALUATION; with no filter there is nothing to
    # bound, and the mail must flow (spec §4).
    org, mailbox, mailbox_id = _persisted(store, subject_filter=None)
    session = FakeSession(raws=[(1, build_raw(subject="x" * 1200, message_id="<m1@x>",
                                             attachments=[("a.pdf", b"%PDF-1")]))])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)
    client = StubClient()

    result = await poll_mailbox_once(store, client, mailbox, org)

    assert result.documents_uploaded == 1 and result.subjects_too_long == 0


async def test_subject_too_long_and_timeout_are_never_both_counted(store, monkeypatch):
    # One terminal disposition per message across the retry loop (spec §8).
    # uid 1 is over the cap; uid 2 is the catastrophic pattern on a long body.
    org, mailbox, mailbox_id = _persisted(store, subject_filter="(a+)+$")
    session = FakeSession(raws=[
        (1, build_raw(subject="x" * 1001, message_id="<m1@x>")),
        (2, build_raw(subject="a" * 60, message_id="<m2@x>")),
    ])
    monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

    result = await poll_mailbox_once(store, StubClient(), mailbox, org)

    assert result.subjects_too_long + result.messages_scanned >= 2
    for uid in (1, 2):
        assert uid in session.seen               # each message retired exactly once
    assert len(session.seen) == len(set(session.seen))
```

The last test asserts the *disposition* invariant (a uid is marked seen exactly once, whatever decided it) rather than a counter value, because the counter is at-least-once by contract and asserting an exact total here would fossilise a crash-replay artifact.

- [ ] **Step 6: Confirm green, then commit**

Run: `.venv/bin/ruff check . && .venv/bin/pytest -q -m "not integration"`

```bash
git add papra_suite/mail.py papra_suite/store.py tests/test_mail_parsing.py tests/test_mail_poller.py tests/test_store.py
git commit -m "feat(filter): SUBJECT_TOO_LONG, and the end of truncated matching

Truncating to MAX_SUBJECT_CHARS before matching was wrong in both directions:
^Rechnung.*\\d{3}$ matched a 1050-character subject whose real text has no
three-digit run, and missed a 1109-character subject that ends in one. The
subject is now kept whole, the filter is simply not asked past the cap, and the
message gets its own disposition instead of a laundered NOMATCH — because
NOMATCH is indistinguishable from a filter that legitimately excludes mail, and
the failure mode is a mailbox that quietly misses every invoice.

Two caps, because they bound different things: MAX_RAW_SUBJECT_HEADER_CHARS
(32768) on the UNDECODED header bounds the decoder's work — encoded-word filler
is free at the wire level, so raw length is not a proxy for decoded length in
either direction — and MAX_SUBJECT_CHARS (1000) bounds what the engine evaluates
against. Over-retirement is accepted at the raw cap and pinned.

The counter is explicitly approximate: mark_seen and the write are separate
steps and fetch_since does not filter \\Seen, so a crash between them re-counts.
What the disposition must never do is disable the mailbox — a TIMEOUT or an
over-long subject is the sender's fault, and 3-strike disablement would let any
sender switch a mailbox off by sending long subjects."
```

---

### Task 8: One admission boundary for both poll paths

**Files:**
- Modify: `papra_suite/mail.py` (`validate_subject_filter`, new `admit_subject_filter`, cache)
- Modify: `papra_suite/pipeline.py`
- Test: `tests/test_mail_poller.py`, `tests/test_pipeline.py`

**Interfaces:**
- Consumes: `KillableMatcher.analyze` (Task 5).
- Produces:

```python
SUBJECT_FILTER_FLAGS = re.IGNORECASE      # the flags the filter is compiled with (spec §6.3)

async def admit_subject_filter(matcher: KillableMatcher, pattern: str | None,
                               *, timeout_s: float = SUBJECT_VALIDATION_TIMEOUT_S) -> None
    # raises ValueError (rejected) or MatcherUnavailable (cannot decide)

def clear_subject_filter_cache() -> None   # for tests and for a policy bump
```

- [ ] **Step 1: Write the failing tests**

```python
class TestAdmissionBoundary:
    async def test_admission_is_cached_by_pattern_flags_and_policy_version(self, monkeypatch):
        calls: list[str] = []

        async def fake_analyze(pattern, *, timeout_s):
            calls.append(pattern)
            return regexlint.Analysis(True)

        matcher = KillableMatcher()
        monkeypatch.setattr(matcher, "analyze", fake_analyze)
        mail.clear_subject_filter_cache()
        await mail.admit_subject_filter(matcher, "^a+$")
        await mail.admit_subject_filter(matcher, "^a+$")
        assert calls == ["^a+$"]                      # one child round trip, not two

        monkeypatch.setattr(regexlint, "ANALYSIS_POLICY_VERSION", 99)
        await mail.admit_subject_filter(matcher, "^a+$")
        assert calls == ["^a+$", "^a+$"]              # a policy bump cannot reuse the verdict

    async def test_rejections_are_not_cached(self, monkeypatch):
        calls: list[int] = []

        async def fake_analyze(pattern, *, timeout_s):
            calls.append(1)
            return regexlint.Analysis(False, "alternation inside a repetition (R2)")

        matcher = KillableMatcher()
        monkeypatch.setattr(matcher, "analyze", fake_analyze)
        mail.clear_subject_filter_cache()
        for _ in range(2):
            with pytest.raises(ValueError, match="R2"):
                await mail.admit_subject_filter(matcher, "(a|aa)+b")
        assert len(calls) == 2

    def test_validate_subject_filter_no_longer_times_probes(self):
        # The ratio gate is deleted, not kept alongside: it was the M2
        # `(\w+\s?)*` mechanism and it never caught the class it existed for.
        import inspect

        source = inspect.getsource(mail.validate_subject_filter)
        assert "perf_counter" not in source and "GROWTH_RATIO" not in source
        assert "regexlint" in source

    def test_validate_subject_filter_uses_the_analyzer(self):
        with pytest.raises(ValueError, match="R1"):
            mail.validate_subject_filter("(a+)+$")
        mail.validate_subject_filter("^Rechnung\\s*#?\\d{4,10}$")   # must not raise

    async def test_the_direct_ingest_path_enforces_the_same_admission(self, store, monkeypatch):
        # Spec §13: poll_mailbox_once and the pipeline's poll path must not drift.
        org_id = store.create_org(papra_org_id="org_papra", name="Test")
        mailbox_id = store.create_mailbox(
            org_id=org_id, host="imap.test", port=993, username="u", password="p",
            subject_filter="(a+)+$",
        )
        session = FakeSession(raws=[(1, build_raw(subject="a" * 40))])
        monkeypatch.setattr("papra_suite.mail.MailboxSession", lambda **kw: session)

        result = await poll_mailbox_once(store, StubClient(),
                                        store.get_mailbox(mailbox_id), store.get_org(org_id))

        assert result.error is not None and "R1" in result.error
        assert store.get_mailbox(mailbox_id).last_error is not None
        assert session.seen == []                       # nothing was decided, nothing retired

    async def test_validate_and_analyze_agree_because_they_are_one_code_path(self):
        # The M2 trap: a filter the dashboard accepts and the poller refuses.
        matcher = KillableMatcher()
        try:
            for pattern in ["(a+)+$", "(a|aa)+b", "^a*a*a*a*b$", "^\\d{4}$", "^Q-?\\d{5}$"]:
                saved = None
                try:
                    await matcher.validate(pattern, timeout_s=2.0)
                except ValueError as error:
                    saved = str(error)
                admitted = await matcher.analyze(pattern, timeout_s=2.0)
                assert (saved is None) == admitted.ok, pattern
        finally:
            await matcher.aclose()
```

- [ ] **Step 2: Run them to verify they fail**

Run: `.venv/bin/pytest -q tests/test_mail_poller.py -k TestAdmissionBoundary`
Expected: FAIL — `admit_subject_filter` missing; `validate_subject_filter` still contains `perf_counter`.

- [ ] **Step 3: Implement admission, replace the ratio gate**

```python
# papra_suite/mail.py
from papra_suite import regexlint

SUBJECT_FILTER_FLAGS = re.IGNORECASE
# key = (pattern, flags, policy version): a judgement made under another policy
# version must not be reusable, and the only invalidation path the store had
# (compare the pattern text) cannot express a rule change (spec §6.3)
_admitted_filters: set[tuple[str, int, int]] = set()


def clear_subject_filter_cache() -> None:
    """Drop every cached admission. Called by tests and by a policy bump."""
    _admitted_filters.clear()


def validate_subject_filter(pattern: str | None, *,
                           deadline_s: float = 0.25) -> None:
    """Reject a subject filter by SHAPE. Raises ValueError; runs in the child.

    The probe-and-ratio gate is gone. It measured growth on short inputs, which
    is structurally unable to detect the class it existed for — `^(a|b)+x$` is
    linear on every probe and exponential at 60 000 characters, and a short probe
    measures an upper bound at that length rather than the worst case the cap
    allows (spec §4.1). What replaces it is `papra_suite.regexlint`: fail-closed
    traversal plus four structural rules, whose guarantee is that a REJECTION is
    sound, not that acceptance means cheap.
    """
    if not pattern:
        return
    analysis = regexlint.analyze(pattern, SUBJECT_FILTER_FLAGS, deadline_s=deadline_s)
    if not analysis.ok:
        raise ValueError(f"subject filter rejected: {analysis.reason}")


async def admit_subject_filter(matcher: KillableMatcher, pattern: str | None,
                               *, timeout_s: float = SUBJECT_VALIDATION_TIMEOUT_S) -> None:
    """The single admission boundary for a configured filter. Both poll paths call it.

    Positive verdicts are cached per (pattern, flags, policy version) so a busy
    mailbox does not pay a child round trip per poll; rejections are not cached,
    so an operator fixing a pattern is never told the old one is still broken.
    MatcherUnavailable propagates: an undecided admission stops the poll, which
    is the only place an unusable filter is refused at all.
    """
    if not pattern:
        return
    key = (pattern, SUBJECT_FILTER_FLAGS, regexlint.ANALYSIS_POLICY_VERSION)
    if key in _admitted_filters:
        return
    analysis = await matcher.analyze(pattern, timeout_s=timeout_s)
    if not analysis.ok:
        raise ValueError(f"subject filter rejected: {analysis.reason}")
    if len(_admitted_filters) > 512:            # bounded, this runs per poll
        _admitted_filters.clear()
    _admitted_filters.add(key)
```

Replace the `await matcher.validate(mailbox.subject_filter, timeout_s=SUBJECT_VALIDATION_TIMEOUT_S)` call in `_poll_mailbox_once` with `await admit_subject_filter(matcher, mailbox.subject_filter)` (same position, inside the same pre-connect `try`, so an existing error path still catches it). Delete `SUBJECT_FILTER_PROBE_PAIRS`, `SUBJECT_FILTER_MAX_GROWTH_RATIO`, and `SUBJECT_FILTER_PROBE_CAP_S`; keep `SUBJECT_VALIDATION_TIMEOUT_S` (it is now the analysis budget).

- [ ] **Step 4: Enforce it on the second path**

In `papra_suite/pipeline.py`, wherever a direct ingest or single-shot poll reaches the store without going through `_poll_mailbox_once`, add the same call before any message is processed:

```python
    # The admission rule follows the filter, not the call site: a pattern that
    # the poller would refuse must not be runnable through this path either
    # (spec §13). MatcherUnavailable and ValueError both stop this call.
    await admit_subject_filter(matcher, mailbox.subject_filter)
```

Then have the child's `analyze`/`validate` arm pass the flags so the child and the cache agree:

```python
        if operation in ("validate", "analyze"):
            from papra_suite.mail import SUBJECT_FILTER_FLAGS, validate_subject_filter

            validate_subject_filter(pattern, flags=SUBJECT_FILTER_FLAGS,
                                    deadline_s=_analysis_deadline_s(request))
```

and give `validate_subject_filter` the matching signature `def validate_subject_filter(pattern: str | None, *, flags: int = SUBJECT_FILTER_FLAGS, deadline_s: float = 0.25) -> None`, passing both to `regexlint.analyze(pattern, flags, deadline_s=deadline_s)`; the `validate` op in `papra_suite/regex_worker.py` now points at the same helper the `analyze` op uses, which is what the agreement test pins:

```python
        if operation == "validate":
            # one admission function for both ops, so a pattern cannot be
            # admissible at save time and refused at poll time (spec §13)
            from papra_suite.mail import validate_subject_filter

            try:
                validate_subject_filter(pattern, deadline_s=_analysis_deadline_s(request))
            except AnalysisTimeout:
                return {"id": request_id, "analysis": "timeout",
                        "policy": ANALYSIS_POLICY_VERSION}
            return {"id": request_id, "ok": True, "policy": ANALYSIS_POLICY_VERSION}
```

(`from papra_suite.regexlint import ANALYSIS_POLICY_VERSION, AnalysisTimeout` moves to the top of `regex_worker.py`'s lazy-import block; the `analyze` arm from Task 5 becomes a call to the same `validate_subject_filter` + the same reason formatting, so there is exactly one place that decides.)

- [ ] **Step 5: Confirm green, then commit**

Run: `.venv/bin/ruff check . && .venv/bin/pytest -q -m "not integration"`
Expected: green. Expect to update existing tests that asserted probe/ratio behaviour — delete those assertions and note the reason in the commit; do not rewrite them to assert the new rule by accident.

```bash
git add papra_suite/mail.py papra_suite/pipeline.py tests/test_mail_poller.py tests/test_pipeline.py
git commit -m "feat(filter): one admission boundary, and the ratio gate is gone

validate_subject_filter now rejects by shape through the analyzer. The
probe-and-ratio gate is deleted rather than kept as a second opinion: it was the
mechanism added for M2's (\\w+\\s?)* and it never caught the class it existed
for, while a timing gate on short inputs structurally cannot — ^(a|b)+x$ is
linear on every probe.

Admission is cached on (pattern, flags, policy version). A positive verdict is
the only cached thing, so an operator who fixes a rejected pattern is never told
the old one is still broken; and the policy version is in the key because the
store's only invalidation path was comparing the pattern text, which cannot
express a rule change. Both poll paths now call the same boundary, because
'both validate, but only one enforces' is how a mailbox ends up with a filter the
save endpoint would not accept."
```

---

### Task 9: The checker, the gate, and the mutation pins

**Files:**
- Create: `scripts/filter_contract_check.py`, `scripts/filter_contract_gate.sh`
- Modify: `tests/test_filter_contract.py` (the corpora and loader arrived in Task 2)

- [ ] **Step 1: Verify the corpora still match the appendix**

Re-extract Appendix A.1/A.2 from the spec and compare them against the two data
files; the sets must be identical. If they differ, one side drifted, and the drift
is the finding — do not "fix" a data file by editing the spec.

- [ ] **Step 2: Write the failing test that reads them**

```python
# appended to tests/test_filter_contract.py
import re
import subprocess
import sys
from pathlib import Path

from papra_suite import regexlint


def test_every_must_reject_pattern_is_rejected():
    assert [p for p in MUST_REJECT if regexlint.analyze(p, re.IGNORECASE).ok] == []


def test_every_must_accept_pattern_is_accepted():
    assert [(p, regexlint.analyze(p, re.IGNORECASE).reason)
            for p in MUST_ACCEPT if not regexlint.analyze(p, re.IGNORECASE).ok] == []


def test_no_rejection_in_either_corpus_comes_from_an_accident():
    # A reason of "failed closed" means a rule did not fire and an exception
    # landed in the wrapper: the pattern was refused by luck, not by policy.
    assert [p for p in MUST_REJECT + MUST_ACCEPT
            if "failed closed" in regexlint.analyze(p, re.IGNORECASE).reason] == []


def test_the_accept_side_contains_the_realistic_filters():
    assert any("Rechnung" in p for p in MUST_ACCEPT)
    assert len(MUST_ACCEPT) >= 39 and len(MUST_REJECT) >= 29


def test_checker_script_agrees_with_the_library():
    out = subprocess.run([sys.executable, "scripts/filter_contract_check.py", "--corpora"],
                         capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stdout + out.stderr


def test_checker_script_fails_when_a_rule_is_broken(monkeypatch, tmp_path):
    # The runner must be able to fail: a checker that cannot report a regression
    # is decoration.
    script = Path("scripts/filter_contract_check.py").read_text().replace(
        "from papra_suite import regexlint",
        "from papra_suite import regexlint\nregexlint.analyze = lambda *a, **k: regexlint.Analysis(True)",
    )
    broken = tmp_path / "broken.py"
    broken.write_text(script)
    out = subprocess.run([sys.executable, str(broken), "--corpora"],
                         capture_output=True, text=True, check=False, cwd=Path.cwd())
    assert out.returncode != 0
```

- [ ] **Step 3: Run them to verify they fail**

Run: `.venv/bin/pytest -q tests/test_filter_contract.py`
Expected: FAIL — the data files and the script do not exist.

- [ ] **Step 4: Write the checker and the gate**

```python
# scripts/filter_contract_check.py
"""Ask the contract about one pattern, or re-run the corpora of record.

A dashboard save pre-flight calls check(); CI calls --corpora. Both go through
this file so a rule change cannot pass in one place and fail in the other.
"""
import argparse
import re
import sys
from pathlib import Path

from papra_suite import regexlint

DATA = Path(__file__).resolve().parent.parent / "tests" / "data"


def check(pattern: str) -> tuple[bool, str]:
    analysis = regexlint.analyze(pattern, re.IGNORECASE)
    return analysis.ok, analysis.reason


def _lines(name: str) -> list[str]:
    return [line.strip() for line in (DATA / name).read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pattern")
    parser.add_argument("--corpora", action="store_true")
    args = parser.parse_args(argv)
    if args.pattern:
        ok, reason = check(args.pattern)
        print(("ACCEPTED " if ok else "REJECTED ") + args.pattern + (f" — {reason}" if reason else ""))
        return 0 if ok else 1
    failures = []
    for pattern in _lines("subject_filter_must_reject.txt"):
        if check(pattern)[0]:
            failures.append(f"must-reject but accepted: {pattern}")
    for pattern in _lines("subject_filter_must_accept.txt"):
        ok, reason = check(pattern)
        if not ok:
            failures.append(f"must-accept but rejected ({reason}): {pattern}")
    for line in failures:
        print(line)
    print(f"corpora: {len(failures)} failures")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
```

```bash
# scripts/filter_contract_gate.sh
#!/usr/bin/env bash
# The analyzer depends on re._parser, which is CPython implementation detail:
# "3.12" is a floor, not a definition (spec §9). Runs the corpora on every
# interpreter this machine has and fails, naming the interpreter, if ANY of them
# disagrees. A green run here does NOT claim an interpreter nobody ran.
set -u
status=0
for py in .venv/bin/python python3.14 python3.13 python3.12 python3; do
  command -v "$py" >/dev/null 2>&1 || [ -x "$py" ] || continue
  if out=$("$py" scripts/filter_contract_check.py --corpora 2>&1); then
    echo "PASS $py"
  else
    echo "FAIL $py"
    echo "$out" | sed 's/^/    /'
    status=1
  fi
done
exit "$status"
```

- [ ] **Step 5: Run the gate and the suite**

Run: `chmod +x scripts/filter_contract_gate.sh && .venv/bin/python scripts/filter_contract_check.py --corpora && ./scripts/filter_contract_gate.sh`
Expected: `corpora: 0 failures`, then `PASS` for every interpreter present. Then:
Run: `.venv/bin/ruff check . && .venv/bin/pytest -q -m "not integration" && ./scripts/filter_contract_gate.sh`
Expected: green; state **which interpreters actually ran** in the PR description (spec §9).

- [ ] **Step 6: Add the mutation pins for the guards no positive fixture exercises**

Append one test per guard, each deleting the guard and asserting the verdict then
flips. A guard nobody can break is decoration. These four are the mistakes that were
actually made in real drafts of this analyzer, one per draft:

```python
def test_blinding_r2_accepts_the_r2_only_witness(monkeypatch):
    monkeypatch.setattr(regexlint, "_contains_branch", lambda node, depth=0: False)
    assert regexlint.analyze("(?:(a)|a)+b", re.IGNORECASE).ok        # was: R2


def test_reading_a_repetition_payload_as_a_body_blinds_everything(monkeypatch):
    # The original shape bug: payload[0] is the int `min`, so a naive walk finds
    # no children and (a+)+ walks away clean.
    def naive(node):
        from papra_suite.regexlint import _is_node, _payload, _seq
        return [c for c in _seq(_payload(node)) if _is_node(c)], set()

    monkeypatch.setattr(regexlint, "_children", naive)
    assert regexlint.analyze("(a+)+$", re.IGNORECASE).ok             # was: R1


def test_restoring_the_width_sum_bug_produces_an_accidental_rejection(monkeypatch):
    # Fail-closed catching a TypeError is not a rule firing; this is how the
    # difference gets detected rather than admired.
    def buggy(widths):
        if any(w is None for w in widths):
            return None
        return (sum(w[0] for w in widths), sum(w[1] for w in widths))

    monkeypatch.setattr(regexlint, "_sum_widths", buggy)
    assert any("failed closed" in regexlint.analyze(p, re.IGNORECASE).reason
               for p in ["(a+)+$", "(?=^(a+)+$)", "(a*)*"])


def test_disabling_r4_accepts_the_adjacent_repetition_class(monkeypatch):
    monkeypatch.setattr(regexlint, "_unbounded_in_run", lambda items: 0)
    assert regexlint.analyze("^a*a*a*a*b$", re.IGNORECASE).ok        # was: R4
```

- [ ] **Step 7: Confirm green, then commit**

Run: `.venv/bin/ruff check . && .venv/bin/pytest -q -m "not integration"`

```bash
git add tests/data scripts tests/test_filter_contract.py
git commit -m "test(filter): corpora of record, a checker, and a per-interpreter gate

The accepted language is a contract with real consequences — M2 shipped
(\\w+\\s?)* as a README example — so it lives in files that CI runs rather than
in prose, and the same file feeds a dashboard save pre-flight. The checker is
tested by breaking a rule: a runner that cannot report a regression is decoration.

The corpora are Python's accepted language, not Papra's. Nothing here guarantees
a German operator's pattern survives: `^Rechnung \\d{1,6} - [A-ZÄÖÜ]{2,3}\$` has
five unbounded repetitions and R4 refuses it. That is the cost of this policy and
it is a residual, not a surprise (spec §11.9).

The gate runs the corpora on every interpreter on the machine because the
analyzer leans on re._parser, which is CPython implementation detail; a green run
only claims the interpreters that ran, so the PR has to name them."
```

---

## Self-Review (completed during planning)

**Spec coverage.** §2 guarantee → Tasks 6 (per-message budget) + 5/1 (fail-closed analysis). §3 rules → R1 Task 2, R2/R3 Task 3, R4 Task 4, bounds/traversal Task 1, with the §3.1 prerequisites (assertions, list nodes, per-branch width, forest counting, `re.IGNORECASE`, unknown opcode) in Tasks 1 and 3. §4 caps → Task 7 (both caps, both directions of the truncation bug, first-header note). §5 layering → Task 5 (analyze in the child, distinct `ANALYSIS_TIMEOUT` frame). §6 budget and caching → Tasks 6 and 8 (policy-version cache identity). §7 M4 → deferred: the dashboard's save-time pre-flight calls `scripts/filter_contract_check.check()` and M4 renders `reason`; the spec's request that saving be *blocked* server-side rather than hidden behind a warning is honoured because `admit_subject_filter` is the same function the poll uses. §8 dispositions and the approximate counter → Task 7 (one terminal disposition, no `NOMATCH`, no disable path, counter documented as at-least-once). §9 pins → corpora Task 9, witnesses Tasks 1–4, the shape/mutation pins in Tasks 1, 2, 3 and 9, per-interpreter gate Task 9. §12 out-of-scope respected: no VM, no dependency, no dashboard.

**Deliberate deviations.** (1) The analyzer lives in a new `regexlint.py` rather than inside `mail.py`, because `mail.py` is already 700+ lines and the analyzer must be importable by the child, the script, and M4 without dragging `fitz` in. (2) `models.py` is named in the spec's table; the models live in `store.py`, so the `Mailbox` field goes there. (3) The corpora of record move from Task 9 to Task 2: the first rule is the first thing that needs them, and a plan that tests R1 against fixtures invented inside the test file re-invents the accepted language on every task. (4) Every fixture list in Tasks 1–4 was **executed against the finished analyzer** on CPython 3.12.15 instead of written from reasoning about it. Four lists drafted from memory were wrong — `?`-exempt patterns asserted as rejects, `^a*a*#?a*$` asserted as an accept, a look-behind witness that CPython refuses to parse at all ("look-behind requires fixed-width pattern"), and a counting witness (`x{2}*`) that is a parse error — and all four are corrected here rather than left for the implementer to discover.

**Type consistency checked across tasks:** `Analysis(ok, reason)` used identically in Tasks 1, 5, 8; `KillableMatcher.analyze(pattern, *, timeout_s) -> Analysis` defined in Task 5, consumed in Task 8; `regexlint.analyze(pattern, flags=0, *, deadline_s=0.25)` consistent in every task; `SubjectTooLong`, `ParsedMessage.subject_too_long`, `PollResult.subjects_too_long`, `Mailbox.subject_too_long_count`, `Store.record_subject_too_long` all introduced in Task 7 and referenced nowhere earlier; `_died_at_self_deadline` defined and used only in Task 6; `_attempt_budget` likewise.

**Known follow-ups this plan does NOT take on** (carry them into the M4 plan): the dashboard must render `Analysis.reason` verbatim; a policy-version bump should call `mail.clear_subject_filter_cache()` from the reconciler's invalidation path (spec §6.3) — the version-in-key already prevents reuse, the explicit clear is what frees memory and makes the invalidation observable; and `scripts/filter_contract_check.check()` is the save pre-flight the dashboard's save action must call.
