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
import os
import signal
import sys
import time

import pytest

from papra_suite.safematch import KillableMatcher, MatchOutcome

# The anchored-literal class: its ambiguous group sits behind the literal, so
# validate_subject_filter's probes (which never carry that literal) measure
# nothing and the pattern PASSES validation. It still costs ~2**n on a
# matching prefix — this is the residual the killable child exists to bound.
CATASTROPHIC = r"(Rechnung|Invoice)\s*(\d+\s*)+$"
CRAFTED_SUBJECT = "Rechnung " + "1" * 40 + "!"
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
    poll loop and must never raise out of search()."""
    matcher = KillableMatcher(command=[sys.executable, "-c", "import sys; sys.exit(3)"])
    try:
        outcome = await asyncio.wait_for(
            matcher.search("needle", "the needle", timeout_s=5.0), timeout=60
        )
        assert outcome is MatchOutcome.UNAVAILABLE
        # validation cannot run either: it degrades to "skipped" (the killable
        # per-message search is what protects the process), never a crash
        assert await asyncio.wait_for(
            matcher.validate(GATED_PATTERN, timeout_s=5.0), timeout=60
        ) is None
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
