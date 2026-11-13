"""Unit tests for the killable regex child behind the subject filter.

THE CONTRACT: an operator-supplied regular expression can burn CPU for
longer than any timeout we are willing to wait, and no timeout inside THIS
process can interrupt it (re.search holds the GIL until it drains). The only
primitive that can stop it is a signal to a separate process, delivered by
the kernel. So: every match runs in a disposable child, a blown budget
KILLS the child instead of waiting for it, the event loop stays free while
the child burns CPU, a dead or garbled child is replaced transparently with
bounded retries, and teardown leaves no orphan process behind.
"""

import asyncio
import contextlib
import os
import signal
import sys
import time

import pytest

import papra_suite.safematch as safematch
from papra_suite.safematch import KillableMatcher, MatcherUnavailable, MatchOutcome

# The anchored-literal class: its ambiguous group sits behind the literal, so
# validate_subject_filter's probes (which never carry that literal) measure
# nothing and the pattern PASSES validation. It still costs ~2**n on a
# matching prefix — this is the residual the killable child exists to bound.
CATASTROPHIC = r"(Rechnung|Invoice)\s*(\d+\s*)+$"
CRAFTED_SUBJECT = "Rechnung " + "1" * 40 + "!"
# The per-request budget these tests hand the child (kept short so the
# self-deadline fires inside the test, not after a real 2s poll).
SUBJECT_BUDGET_S = 2.0
# The class the ratio gate does catch (probe near-misses hit its ambiguity).
GATED_PATTERN = r"(a|aa)+b"


class SlowStartMatcher(KillableMatcher):
    """A matcher whose child spawn can be held open, so the close/spawn and
    cancel/spawn races can be driven deterministically."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.spawn_entered = asyncio.Event()
        self.spawn_release = asyncio.Event()
        self.spawned = []

    async def _spawn(self):
        self.spawn_entered.set()
        await self.spawn_release.wait()
        process = await super()._spawn()
        self.spawned.append(process)
        return process


async def test_search_reports_match_and_nomatch_and_honours_ignorecase():
    matcher = KillableMatcher()
    try:
        assert await matcher.search(".*rechnung.*", "Rechnung 42",
                                    timeout_s=10.0) is MatchOutcome.MATCH
        assert await matcher.search(".*rechnung.*", "Newsletter",
                                    timeout_s=10.0) is MatchOutcome.NOMATCH
        # the sync subject_matches uses re.IGNORECASE; the child must agree
        assert await matcher.search("RECHNUNG", "Rechnung 42",
                                    timeout_s=10.0) is MatchOutcome.MATCH
    finally:
        await matcher.aclose()


async def test_child_starts_lazily_and_aclose_reaps_it():
    # a mailbox without a subject filter must cost nothing: no child, no
    # import of the regex machinery, until the first real request
    matcher = KillableMatcher()
    assert matcher.pid is None
    await matcher.validate(None, timeout_s=10.0)
    await matcher.validate("", timeout_s=10.0)
    assert matcher.pid is None

    assert await matcher.search("needle", "the needle", timeout_s=10.0) \
        is MatchOutcome.MATCH
    assert matcher.pid is not None

    await matcher.aclose()
    assert matcher.pid is None
    assert matcher.child_returncode is not None   # waited on, not abandoned
    await matcher.aclose()                        # idempotent


async def test_timeout_kills_the_child_instead_of_draining_it():
    """The whole point: a search that cannot finish is KILLED, and the caller
    learns about it as an outcome (no exception for control flow)."""
    matcher = KillableMatcher()
    try:
        start = time.monotonic()
        outcome = await matcher.search(CATASTROPHIC, CRAFTED_SUBJECT, timeout_s=1.0)
        elapsed = time.monotonic() - start
    finally:
        await matcher.aclose()

    assert outcome is MatchOutcome.TIMEOUT
    # ~1s budget + process teardown; the search itself would not finish in
    # minutes (2**40 steps), so a late-but-honest result here means the kill
    # did not happen
    assert elapsed < 15.0
    assert matcher.child_returncode is not None   # the child was killed+reaped


async def test_child_is_respawned_transparently_after_a_timeout():
    matcher = KillableMatcher()
    try:
        assert await matcher.search(CATASTROPHIC, CRAFTED_SUBJECT,
                                    timeout_s=1.0) is MatchOutcome.TIMEOUT
        dead_pid = matcher.pid
        assert await matcher.search(".*rechnung.*", "Rechnung 42",
                                    timeout_s=10.0) is MatchOutcome.MATCH
        assert matcher.pid is not None
        assert matcher.pid != dead_pid            # a fresh child, not the zombie
    finally:
        await matcher.aclose()


async def test_child_killed_between_requests_is_retried_on_the_respawn():
    """A child that dies (OOM-killed, crashed) must be invisible to callers:
    the request is retried once against the replacement child."""
    matcher = KillableMatcher()
    try:
        assert await matcher.search("needle", "the needle", timeout_s=10.0) \
            is MatchOutcome.MATCH
        first_pid = matcher.pid
        os.kill(first_pid, signal.SIGKILL)        # simulate a crash
        await asyncio.sleep(0.05)                 # let the OS close the pipes
        assert await matcher.search("needle", "the needle", timeout_s=10.0) \
            is MatchOutcome.MATCH
        assert matcher.pid not in (None, first_pid)
    finally:
        await matcher.aclose()


async def test_a_child_that_dies_every_time_degrades_without_hanging():
    """Bounded retries, then degrade: a broken child must never wedge the
    poll loop and must never raise out of search().

    CONTRACT CHANGE (round-4 review, decided deliberately): validate() no
    longer degrades to "skipped". An unusable child means the gate decided
    NOTHING, and a mailbox that polls green with an inert filter is worse than
    one that fails: search() still reports UNAVAILABLE (never raises), while
    validate() raises MatcherUnavailable so the caller can fail the poll. The
    assertion below got stricter, not looser; test_mail_poller asserts the
    poll-site consequence (mail un-retired, cursor frozen, failure counted).
    """
    matcher = KillableMatcher(command=[sys.executable, "-c", "import sys; sys.exit(3)"])
    try:
        outcome = await asyncio.wait_for(
            matcher.search("needle", "the needle", timeout_s=5.0), timeout=60
        )
        assert outcome is MatchOutcome.UNAVAILABLE
        # validation cannot run either, and that is a FAILURE, not a pass:
        # a filter that was never evaluated must not look like a safe filter
        with pytest.raises(MatcherUnavailable):
            await asyncio.wait_for(matcher.validate(GATED_PATTERN, timeout_s=5.0), timeout=60)
    finally:
        await matcher.aclose()


async def test_unparseable_child_output_is_treated_as_a_dead_child():
    """stdout is the protocol channel: a stray print from the child (or from
    anything it imports) garbles a reply, which must mean "this child is
    dead to us", not "desync forever" and not a hang."""
    # flush=True matters: stdout is a pipe here, so an unflushed print would
    # sit in the buffer and the parent would see a timeout instead of garbage
    chatter = (
        "import sys\n"
        "for line in sys.stdin:\n"
        "    print('diagnostic leak', flush=True)\n"
    )
    matcher = KillableMatcher(command=[sys.executable, "-c", chatter])
    try:
        outcome = await asyncio.wait_for(
            matcher.search("needle", "the needle", timeout_s=5.0), timeout=60
        )
        assert outcome is MatchOutcome.UNAVAILABLE
    finally:
        await matcher.aclose()


async def test_closed_matcher_stops_spawning_children():
    matcher = KillableMatcher()
    assert await matcher.search("needle", "the needle", timeout_s=10.0) \
        is MatchOutcome.MATCH
    await matcher.aclose()

    # aclose() is teardown, not pause: a closed matcher must not resurrect a
    # child behind its owner's back (that is how orphan processes are born)
    assert await matcher.search("needle", "the needle", timeout_s=5.0) \
        is MatchOutcome.UNAVAILABLE
    assert matcher.pid is None


async def test_a_child_that_ignores_sigterm_is_escalated_to_sigkill():
    """The teardown escalation pin: terminate() -> grace -> kill(). A child
    that traps SIGTERM (or is wedged) must still die inside budget + grace,
    and must be gone from the process table afterwards — a matcher that only
    asks politely is how a serve shutdown leaks a CPU burner.
    """
    stubborn = (
        "import signal, sys, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "for line in sys.stdin:\n"
        "    time.sleep(3600)\n"   # never answers, never dies politely
    )
    matcher = KillableMatcher(command=[sys.executable, "-c", stubborn])
    request = asyncio.create_task(matcher.search("needle", "needle", timeout_s=0.5))
    for _ in range(2000):
        if matcher.pid is not None:
            break
        await asyncio.sleep(0.005)
    assert matcher.pid is not None
    child_pid = matcher.pid

    start = time.monotonic()
    assert await request is MatchOutcome.TIMEOUT
    elapsed = time.monotonic() - start
    assert elapsed < 5.0                     # budget 0.5s + 0.5s grace, not 3600s
    # teardown is not "signal sent": the SIGKILL is waited on too, so by the
    # time search() returns the child has an exit status
    assert matcher.child_returncode == -9    # killed by SIGKILL, and reaped
    with pytest.raises(ProcessLookupError):  # and gone from the process table
        os.kill(child_pid, 0)
    await matcher.aclose()


async def test_aclose_during_a_child_spawn_kills_the_child_it_created():
    """The close/spawn race pin: aclose() does not wait on the request lock
    (teardown must never be held hostage by a hung match), so a spawn that is
    still in flight when the owner closes must not hand a live child to a dead
    matcher. Deterministic: the spawn is held open, aclose runs to completion,
    then the spawn is released.
    """
    matcher = SlowStartMatcher()
    request = asyncio.create_task(matcher.search("needle", "the needle",
                                                 timeout_s=10.0))
    await matcher.spawn_entered.wait()

    await matcher.aclose()                  # completes while the spawn hangs
    assert matcher.spawned == []            # nothing exists to close yet
    matcher.spawn_release.set()

    # the late child is killed by the matcher that created it, and the request
    # degrades instead of running on a matcher nobody owns any more
    assert await asyncio.wait_for(request, timeout=30) is MatchOutcome.UNAVAILABLE
    assert len(matcher.spawned) == 1
    late_child = matcher.spawned[0]
    assert late_child.returncode is not None
    with pytest.raises(ProcessLookupError):
        os.kill(late_child.pid, 0)
    await matcher.aclose()                  # still idempotent


async def test_a_spawn_that_outlives_a_cancelled_request_still_dies():
    """Ownership pin for the cancel/spawn race: a request cancelled while its
    child is being created must not leave a child that belongs to nobody —
    that is exactly the serve-shutdown moment. The spawn is shielded and its
    result is destroyed by a done-callback, so the guarantee comes from this
    module rather than from wherever CPython happens to deliver the signal
    (3.12 kills a cancelled spawn inside _make_subprocess_transport; earlier
    versions did not). What the test asserts is the contract, whichever path
    gets there first: a child that this matcher created dies by the time the
    request is gone.
    """
    matcher = SlowStartMatcher()
    request = asyncio.create_task(matcher.search("needle", "the needle",
                                                 timeout_s=10.0))
    await matcher.spawn_entered.wait()

    request.cancel()                        # cancelled mid-spawn
    with pytest.raises(asyncio.CancelledError):
        await request
    assert matcher.spawned == []            # the child does not exist yet
    matcher.spawn_release.set()

    late_child = None
    for _ in range(2000):                   # the spawn lands, then dies
        if matcher.spawned:
            late_child = matcher.spawned[0]
            break
        await asyncio.sleep(0.005)
    assert late_child is not None, "the released spawn never produced a child"

    for _ in range(200):
        if late_child.returncode is not None:
            break
        await asyncio.sleep(0.05)
    assert late_child.returncode is not None, \
        f"child {late_child.pid} outlived the request that spawned it"
    with pytest.raises(ProcessLookupError):
        os.kill(late_child.pid, 0)
    await matcher.aclose()


async def test_teardown_always_delivers_sigkill_even_if_it_cannot_reap(monkeypatch):
    """The escalation invariant, driven on purpose through the private
    teardown with a child that never reaps: both give-up paths (grace expires,
    teardown itself cancelled) must still have SENT SIGKILL. "We stopped
    waiting" may never mean "we never killed". These branches are unreachable
    with a real child that dies, which is why this one uses a stand-in.
    """
    import papra_suite.safematch as safematch

    monkeypatch.setattr(safematch, "KILL_GRACE_S", 0.01)
    monkeypatch.setattr(safematch, "REAP_GRACE_S", 0.01)

    class NeverReaps:
        pid = 4242
        returncode = None
        stdin = None

        def __init__(self):
            self.signals = []

        def terminate(self):
            self.signals.append(signal.SIGTERM)

        def kill(self):
            self.signals.append(signal.SIGKILL)

        async def wait(self):
            await asyncio.sleep(3600)          # the reap never completes

    matcher = KillableMatcher()
    process = NeverReaps()
    await matcher._terminate(process)          # both waits give up
    assert process.signals == [signal.SIGTERM, signal.SIGKILL]

    class InterruptedWait(NeverReaps):
        async def wait(self):
            raise asyncio.CancelledError       # shutdown interrupts teardown

    interrupted = InterruptedWait()
    with pytest.raises(asyncio.CancelledError):
        await matcher._terminate(interrupted)  # cancellation is preserved...
    assert interrupted.signals == [signal.SIGTERM, signal.SIGKILL]  # ...after the kill


async def test_validate_rejects_an_unsafe_pattern_without_killing_the_child():
    matcher = KillableMatcher()
    try:
        with pytest.raises(ValueError, match="unsafe"):
            await matcher.validate(GATED_PATTERN, timeout_s=30.0)
        # a rejection is a normal reply, not a kill: the child is still there
        assert await matcher.search(".*x.*", "x", timeout_s=10.0) is MatchOutcome.MATCH
    finally:
        await matcher.aclose()


async def test_validate_reports_an_invalid_regex_as_valueerror():
    matcher = KillableMatcher()
    try:
        with pytest.raises(ValueError, match="invalid subject filter"):
            await matcher.validate("(", timeout_s=10.0)
    finally:
        await matcher.aclose()


async def test_validate_kills_an_unbounded_probe():
    """The validator itself used to be unbounded: (a+)+\\d+$ against the
    "a"*30 probe never returns (measured: still running after 15s), so the
    poll loop froze before a single mail was read. Inside the child the
    probe is killed and the pattern is rejected."""
    matcher = KillableMatcher()
    try:
        start = time.monotonic()
        with pytest.raises(ValueError, match="unsafe"):
            await matcher.validate(r"(a+)+\d+$", timeout_s=2.0)
        elapsed = time.monotonic() - start
        assert elapsed < 20.0
        assert matcher.child_returncode is not None   # killed, not finished
    finally:
        await matcher.aclose()


async def test_validate_accepts_realistic_operator_patterns():
    """No false positives on the admin's vocabulary — including the
    anchored-literal class, which still passes (documented residual: the
    probe alphabet never reaches its ambiguity)."""
    realistic = [
        r".*rechnung.*",
        r"(Rechnung|Invoice)\s*(\d+\s*)+$",
        r"^(Rechnung|Invoice)[ ]*(\d+)+$",
        r"Rechnung.*",
        r"^Invoice ?#?\d{0,10}$",
        r"(?i)(rechnung|invoice|beleg|quittance)",
        r"^(AWV|Lieferschein|Angebot)\s+\d+",
        r"\b\d{3}-\d{3}-\d{4}\b",
        r"^(Re: |WG: |Ant: )*(Rechnung|Angebotsanfrage)\b.*",
        r"^\[.*\] ?Rechnung Nr\. ?\d+$",
        r"(monatlich|monthly)\s*(abrechnung|statement)",
        r"^Kontoauszug\b.{0,60}(NR|No)?\.?\s*\w*$",
    ]
    matcher = KillableMatcher()
    try:
        for pattern in realistic:
            await matcher.validate(pattern, timeout_s=30.0)
    finally:
        await matcher.aclose()


# --- The child bounds ITSELF: the parent may be dead when the budget ends ---

# The parent owns the NUMBER (it sends deadline_s with every request); the
# child owns the ENFORCEMENT. Without it the only watchdog was the parent, and
# `kill -9` on the parent while the child was backtracking left an orphan at
# ppid=1 burning CPU (measured: still running 100 s later, 98 CPU-seconds,
# killed by hand) — and because the cursor had not advanced, every restart
# re-armed and could add another orphan.
ORPHAN_HARNESS = """
import asyncio
import os
import signal

from papra_suite.safematch import KillableMatcher


async def main():
    matcher = KillableMatcher()
    task = asyncio.create_task(matcher.search({pattern!r}, {subject!r},
                                              timeout_s={budget}))
    for _ in range(4000):
        if matcher.pid is not None:
            break
        await asyncio.sleep(0.005)
    print(matcher.pid, flush=True)
    await asyncio.sleep(0.3)            # let the search get properly underway
    os.kill(os.getpid(), signal.SIGKILL)  # the parent the ONLY watchdog was


asyncio.run(main())
"""


async def _read_child_pid(stdout) -> int:
    """One line from the harness: the pid of the child it left behind."""
    line = await asyncio.wait_for(stdout.readline(), timeout=90)
    assert line, "the orphan harness died before reporting its child"
    return int(line)


async def test_an_orphaned_child_kills_itself_when_the_parent_is_sigkilled():
    """The self-deadline is the last line of defence, and it must be the
    child's: SIGALRM has no Python handler installed here, so its default
    disposition terminates the process even mid-re.search, where nothing this
    interpreter could run would ever get a turn.

    Measured before the fix: the harness's SIGKILL left the child at ppid=1
    still burning a core (98 CPU-seconds after 100 s, killed by hand).
    """
    from papra_suite.safematch import _child_env

    harness = ORPHAN_HARNESS.format(pattern=CATASTROPHIC, subject=CRAFTED_SUBJECT,
                                    budget=SUBJECT_BUDGET_S)
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-c", harness,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        env=_child_env(),
    )
    child_pid = await _read_child_pid(proc.stdout)
    armed = time.monotonic()          # the alarm went in with this request
    await asyncio.wait_for(proc.wait(), timeout=30)
    assert proc.returncode == -9, "the harness must die of its own SIGKILL"

    try:
        # the child is still mid-search when the parent dies: it has to be
        # alive to be orphaned, and this is what the self-deadline is for
        os.kill(child_pid, 0)
        deadline = armed + SUBJECT_BUDGET_S + safematch.SELF_DEADLINE_GRACE_S + 6.0
        while True:
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                break
            assert time.monotonic() < deadline, (
                f"orphaned regex child {child_pid} was still running "
                f"{SUBJECT_BUDGET_S}s after its parent was SIGKILLed: the child "
                "has no wall-clock self-deadline and will burn CPU forever"
            )
            await asyncio.sleep(0.05)
    finally:
        with contextlib.suppress(ProcessLookupError):   # never leave a burner
            os.kill(child_pid, signal.SIGKILL)


async def test_an_idle_child_is_not_killed_by_its_own_deadline(monkeypatch):
    """The self-deadline arms around an OPERATION, not around the child's
    life: one child is reused across a whole poll loop's life, so killing an
    idle one between requests would turn the safety net into the outage.

    A tiny grace makes the alarm that WOULD have fired land well inside the
    sleep below; the alarm being cancelled on completion is what keeps the
    same child alive and answering afterwards.
    """
    import papra_suite.safematch as safematch_mod

    monkeypatch.setattr(safematch_mod, "SELF_DEADLINE_GRACE_S", 0.0)
    matcher = KillableMatcher()
    try:
        assert await matcher.search("needle", "the needle", timeout_s=0.5) \
            is MatchOutcome.MATCH
        pid = matcher.pid
        assert pid is not None
        await asyncio.sleep(1.5)        # past the 1s alarm that request armed
        os.kill(pid, 0)                 # still in the process table
        assert matcher.pid == pid
        assert await matcher.search("needle", "the needle", timeout_s=10.0) \
            is MatchOutcome.MATCH
        assert matcher.pid == pid       # the SAME child, never respawned
    finally:
        await matcher.aclose()


async def test_a_request_without_a_deadline_is_still_answered():
    """deadline_s is the parent's number, not a protocol requirement: a
    hand-made request (or a parent from before the field existed) must still
    be served, on the child's own conservative default."""
    import json

    from papra_suite.safematch import DEFAULT_COMMAND, _child_env

    proc = await asyncio.create_subprocess_exec(
        *DEFAULT_COMMAND,
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL, env=_child_env(),
    )
    try:
        proc.stdin.write(json.dumps({"id": 1, "op": "search", "pattern": "needle",
                                     "subject": "the needle", "flags": 2}).encode() + b"\n")
        await proc.stdin.drain()
        reply = json.loads(await asyncio.wait_for(proc.stdout.readline(), timeout=20))
        assert reply == {"id": 1, "matched": True}
    finally:
        proc.kill()
        await proc.wait()


# --- the import path the child actually uses -------------------------------

# A papra_suite/ package that answers "matched" to everything and passes every
# validation: if the child ever imports THIS copy instead of the pinned one,
# the subject filter stops being a gate and reports what an attacker chose.
HOSTILE_WORKER = '''
import json
import sys

for line in sys.stdin:
    if not line.strip():
        continue
    request = json.loads(line)
    if request.get("op") == "validate":
        reply = {"id": request.get("id"), "ok": True}
    else:
        reply = {"id": request.get("id"), "matched": True}
    sys.stdout.write(json.dumps(reply) + "\\n")
    sys.stdout.flush()
'''


def _write_shadow_package(root) -> None:
    package = root / "papra_suite"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "regex_worker.py").write_text(HOSTILE_WORKER, encoding="utf-8")


async def _ask_worker(command, cwd, request: dict) -> dict | None:
    """One raw request to DEFAULT_COMMAND (or a mutant of it) from `cwd`."""
    import json

    from papra_suite.safematch import _child_env

    proc = await asyncio.create_subprocess_exec(
        *command, cwd=str(cwd),
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL, env=_child_env(),
    )
    try:
        proc.stdin.write(json.dumps(request).encode() + b"\n")
        await proc.stdin.drain()
        line = await asyncio.wait_for(proc.stdout.readline(), timeout=20)
        return json.loads(line) if line else None
    finally:
        proc.kill()
        await proc.wait()


async def test_the_child_ignores_a_shadowing_package_in_its_cwd(tmp_path, monkeypatch):
    """`python -m` puts the CWD ahead of the PYTHONPATH pin, so without -P a
    papra_suite/ next to the daemon's working directory answers the operator's
    filter instead of the shipped one — a silent parent/child divergence for
    any script or console-script launch, and the shape the pin's docstring is
    about. -P (PYTHONSAFEPATH) keeps the cwd off sys.path so the pin wins.
    """
    shadow = tmp_path / "shadow"
    _write_shadow_package(shadow)
    monkeypatch.chdir(shadow)
    request = {"id": 1, "op": "search", "pattern": "needle", "subject": "haystack",
               "flags": 2}

    # control: the shadow really does win when the cwd is on the path, so the
    # assertion below cannot pass vacuously
    from papra_suite.safematch import DEFAULT_COMMAND

    shadowed = await _ask_worker([DEFAULT_COMMAND[0], "-m", "papra_suite.regex_worker"],
                                 shadow, request)
    assert shadowed == {"id": 1, "matched": True}

    matcher = KillableMatcher()
    try:
        # the shipped gate answers: the pattern does not match, and the
        # catastrophic pattern is still refused
        assert await matcher.search("needle", "haystack", timeout_s=10.0) \
            is MatchOutcome.NOMATCH
        with pytest.raises(ValueError, match="unsafe"):
            await matcher.validate(GATED_PATTERN, timeout_s=30.0)
    finally:
        await matcher.aclose()


async def test_the_child_resolves_the_package_from_the_env_pin_in_a_foreign_cwd(
    tmp_path, monkeypatch
):
    """The PYTHONPATH pin, tested where it actually matters.

    A test run from the repo root cannot tell the pin from luck: `-m` puts the
    cwd on sys.path, so papra_suite/ would be importable even with no pin at
    all — dropping the package root from _child_env() there survives the whole
    suite. Started from a directory that holds nothing, with PYTHONPATH absent
    from the inherited environment, the pin is the only thing that can resolve
    the worker module, which is the installed/console-script launch the
    docstring says it exists for.
    """
    monkeypatch.delenv("PYTHONPATH", raising=False)
    monkeypatch.chdir(tmp_path)          # no papra_suite anywhere near the child

    matcher = KillableMatcher()
    try:
        assert await matcher.search("needle", "the needle", timeout_s=10.0) \
            is MatchOutcome.MATCH       # a child that could not import answers nothing
    finally:
        await matcher.aclose()
