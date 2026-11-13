"""Killable execution for the operator-supplied mail subject filter.

WHY A CHILD PROCESS. The subject filter is an operator-typed regular
expression, and the catastrophic-backtracking class of those patterns can
burn CPU for an attacker-choosable duration. Nothing inside THIS process can
interrupt one: re.search() holds the GIL for its whole runtime, so an
asyncio.wait_for around it (even around asyncio.to_thread) cannot fire until
the search has already drained — it bounds nothing, and while it "runs" the
whole event loop is frozen (every poll loop, both workers, the sweep). A
separate process is the smallest primitive that is actually killable:
SIGTERM/SIGKILL are delivered by the kernel, not by the Python eval loop, so
they land mid-search. The loop stays free while the child burns CPU.

CONTRACT. Every request either gets an answer or is decided by a kill, and
the caller never blocks past its budget:
  * MatchOutcome.TIMEOUT   — the budget expired, the child was killed
  * MatchOutcome.UNAVAILABLE — no child could be used; the caller degrades
The child is lazy (a mailbox without a filter never spawns one), carries one
request at a time, and is disposable: a blown budget, a crash, an EOF, or a
garbled reply retires it and the next request gets a fresh one — with a
bounded number of retries, so a child that dies on every attempt degrades
instead of looping forever. aclose() kills the child and is idempotent, so
the owner's teardown (serve shutdown, task death, mailbox removal) cannot
leave an orphan behind: teardown returns only once the child has been reaped
(SIGTERM, wait, SIGKILL, longer wait — both signals waited on, and the child
that a cancelled or closed matcher leaves behind is killed by the spawn that
created it, never abandoned).
"""

import asyncio
import contextlib
import enum
import json
import logging
import os
import re
import sys

logger = logging.getLogger(__name__)

# Grace between SIGTERM and SIGKILL. The child blocks in a read or in a
# regex; SIGTERM alone is enough (default disposition, kernel-delivered),
# the grace only covers a child that is wedged in uninterruptible sleep.
KILL_GRACE_S = 0.5
# How long to wait for the reap AFTER a SIGKILL. Longer than the SIGTERM
# grace on purpose: SIGKILL cannot be caught, so this wait only covers the
# kernel handing back a dead child, and giving up on it early would make
# "teardown returned" mean less than "the child is gone".
REAP_GRACE_S = 2.0
# Requests are retried against a replacement child at most this often: a
# child that dies on every attempt must degrade, not spin.
MAX_CHILD_RETRIES = 2

DEFAULT_COMMAND = (sys.executable, "-m", "papra_suite.regex_worker")


class MatchOutcome(str, enum.Enum):
    """Result of one subject-filter match. No exceptions for flow."""

    MATCH = "match"
    NOMATCH = "nomatch"
    TIMEOUT = "timeout"        # the child was killed at the budget
    UNAVAILABLE = "unavailable"  # no child could run the request at all


class MatcherUnavailable(RuntimeError):
    """No child could be started or kept alive long enough to answer."""


class MatchTimeout(RuntimeError):
    """Internal: the reply budget expired and the child was killed."""


def _parse_reply(reply_line: bytes, request_id: int) -> tuple[dict | None, str]:
    """Parse one protocol line: (reply, "") when usable, (None, reason) when not.

    An EOF, an unparseable line, or a reply that answers a different request
    id all mean the same thing — this child's stdout can no longer be
    trusted, so it is dead to us (see the HARD RULE in regex_worker.py).
    """
    if not reply_line:
        return None, "the regex child exited while answering"
    try:
        reply = json.loads(reply_line)
    except (UnicodeDecodeError, ValueError) as error:
        return None, f"the regex child sent a non-JSON line: {error}"
    if not isinstance(reply, dict):
        return None, f"the regex child sent a non-object reply: {reply!r}"
    if reply.get("id") != request_id:
        answered = reply.get("id")
        return None, f"the regex child answered {answered!r} instead of request {request_id}"
    return reply, ""


def _child_env() -> dict:
    """Parent environment plus a PYTHONPATH pin at this package's parent.

    The child is started with `-m papra_suite.regex_worker`, which needs the
    package importable in a fresh interpreter. Depending on how the service
    was launched (installed, `python -m papra_suite`, a test run that gets
    its sys.path from a root conftest) that is not guaranteed by the inherited
    environment alone, and a child that cannot import the package would
    silently degrade every subject filter to non-match.
    """
    import papra_suite

    package_root = os.path.dirname(os.path.dirname(os.path.abspath(papra_suite.__file__)))
    env = dict(os.environ)
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        package_root + os.pathsep + existing if existing else package_root
    )
    return env


class KillableMatcher:
    """Regex matching in a child process that can be killed mid-search."""

    def __init__(self, command=None) -> None:
        # overridable for tests (a child that crashes, chatters, or dies);
        # production always uses the default worker module
        self._command = list(command) if command is not None else list(DEFAULT_COMMAND)
        self._process: asyncio.subprocess.Process | None = None
        # the most recent child object, kept after teardown: its returncode is
        # the evidence that a killed/closed child was reaped, not abandoned
        self._last_process: asyncio.subprocess.Process | None = None
        self._lock = asyncio.Lock()
        self._next_id = 0
        self._closed = False

    @property
    def pid(self) -> int | None:
        """pid of the live child, or None when there is none."""
        process = self._process
        if process is None or process.returncode is not None:
            return None
        return process.pid

    @property
    def child_returncode(self) -> int | None:
        """Returncode of the most recent child (None if never started/not yet
        reaped): a set value after a TIMEOUT or aclose() is the proof that we
        killed and waited for the child instead of walking away from it."""
        process = self._last_process
        return process.returncode if process is not None else None

    async def search(self, pattern: str, subject: str, *, timeout_s: float,
                     flags: int = re.IGNORECASE) -> MatchOutcome:
        """re.search(pattern, subject, flags) in the child, killable at the budget.

        The default flags mirror the sync mail.subject_matches (IGNORECASE) so
        both paths answer the same question; the caller truncates the subject
        to its own cap before asking.

        Returns MATCH/NOMATCH, TIMEOUT when the budget expired (the child is
        dead, the next call respawns), or UNAVAILABLE when no child could run
        at all. Never raises: the poll site must keep ingesting either way.
        """
        try:
            reply = await self._request(
                {"op": "search", "pattern": pattern, "subject": subject, "flags": flags},
                timeout_s=timeout_s,
            )
        except MatchTimeout as error:
            logger.warning("regex child killed: %s", error)
            return MatchOutcome.TIMEOUT
        except MatcherUnavailable as error:
            logger.warning("regex child unavailable: %s", error)
            return MatchOutcome.UNAVAILABLE
        if "error" in reply:
            # an uncompilable or otherwise failing pattern: the poll
            # pre-validation normally stops this, so treat it as a non-match
            # (never as a match: an unrunnable filter must not ingest mail)
            logger.warning("subject filter could not run: %s", reply["error"])
            return MatchOutcome.NOMATCH
        return MatchOutcome.MATCH if reply.get("matched") else MatchOutcome.NOMATCH

    async def validate(self, pattern: str | None, *, timeout_s: float) -> None:
        """Run validate_subject_filter in the child. Raises ValueError on rejection.

        This is what makes the *validator* bounded: its own probes can be
        catastrophic ((a+)+\\d+$ never returns), and a hung validator froze the
        poll loop before a single mail was read. A blown budget is a
        rejection — a pattern whose probe timing does not terminate inside
        the budget is exactly the class the gate exists to refuse.

        An empty pattern needs no validation and must not start a child. If
        no child can run at all the gate is SKIPPED with a WARNING: the
        killable per-message search is what protects the process, so a broken
        child costs the early rejection, never the process.
        """
        if not pattern:
            return
        try:
            reply = await self._request(
                {"op": "validate", "pattern": pattern, "subject": "", "flags": 0},
                timeout_s=timeout_s,
            )
        except MatchTimeout as error:
            raise ValueError(
                f"subject filter is unsafe: validation exceeded {timeout_s}s "
                "and was killed"
            ) from error
        except MatcherUnavailable as error:
            logger.warning(
                "subject filter pre-validation skipped, no regex child available: %s",
                error,
            )
            return
        if "error" in reply:
            raise ValueError(str(reply["error"]))

    async def aclose(self) -> None:
        """Kill the child and mark this matcher closed. Idempotent, never raises.

        Closed means closed: a closed matcher refuses to spawn again, so a
        leaked reference cannot resurrect a process after its owner tore down.
        It deliberately does NOT wait on the request lock — teardown must not
        be held hostage by a hung match; an in-flight request loses its child
        and degrades to UNAVAILABLE instead.
        """
        self._closed = True
        process, self._process = self._process, None
        if process is not None:
            await self._terminate(process)

    async def _request(self, payload: dict, *, timeout_s: float) -> dict:
        """One request/response round trip, respawning the child as needed.

        Raises MatchTimeout (budget blown, child killed — deliberately NOT
        retried: retrying a catastrophic pattern just burns another budget)
        or MatcherUnavailable (dead child after MAX_CHILD_RETRIES respawns).
        """
        async with self._lock:
            attempts = 0
            while True:
                attempts += 1
                process = await self._ensure_process()
                self._next_id += 1
                request_id = self._next_id
                line = json.dumps({"id": request_id, **payload}).encode("utf-8") + b"\n"
                try:
                    process.stdin.write(line)
                    await process.stdin.drain()
                except (OSError, RuntimeError) as error:
                    # the child died between the check and the write
                    await self._retire(process, f"request could not be sent: {error}")
                    if attempts > MAX_CHILD_RETRIES:
                        raise MatcherUnavailable(
                            f"could not send a request to the regex child: {error}"
                        ) from error
                    continue

                try:
                    reply_line = await asyncio.wait_for(
                        process.stdout.readline(), timeout_s
                    )
                except TimeoutError as error:
                    await self._retire(process, f"no reply within {timeout_s}s")
                    raise MatchTimeout(
                        f"the regex child exceeded its {timeout_s}s budget and was killed"
                    ) from error
                except (EOFError, OSError, ValueError) as error:
                    await self._retire(process, f"reading the reply failed: {error}")
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

    async def _ensure_process(self) -> asyncio.subprocess.Process:
        """Return the live child, starting (or restarting) it when needed."""
        if self._closed:
            raise MatcherUnavailable("the matcher is closed")
        if self._process is not None and self._process.returncode is None:
            return self._process
        # The spawn runs as its own shielded task so that the child's
        # ownership never depends on WHERE a cancellation lands. CPython 3.12
        # already kills a child whose spawn wait is cancelled
        # (unix_events._make_subprocess_transport closes the transport), but
        # that is an internal of one CPython version's spawn path; shielded,
        # the child is always ours to destroy: the done-callback below kills
        # anything a spawn produces after its request has stopped waiting.
        spawn = asyncio.ensure_future(self._spawn())
        try:
            process = await asyncio.shield(spawn)
        except asyncio.CancelledError:
            spawn.add_done_callback(self._destroy_late_child)
            raise
        except OSError as error:
            self._process = None
            raise MatcherUnavailable(f"could not start the regex child: {error}") from error
        if self._closed:
            # aclose() landed while this child was being spawned (teardown
            # deliberately does not wait on the request lock): this child has
            # no owner left who would ever close it, so it dies right here
            await self._terminate(process)
            raise MatcherUnavailable("the matcher is closed")
        self._process = process
        self._last_process = process
        logger.debug("started regex child pid=%s", process.pid)
        return process

    def _destroy_late_child(self, spawn) -> None:
        """Kill a child whose spawn completed after we stopped waiting for it.

        Runs as a done-callback, so it executes even though the request that
        asked for the child is gone. The kill is synchronous (a signal does not
        need the event loop); the reap is scheduled best-effort and, if the
        loop is already shutting down, asyncio's child watcher reaps the dead
        child anyway.
        """
        if spawn.cancelled() or spawn.exception() is not None:
            return  # no child was ever created
        process = spawn.result()
        logger.warning("killing a regex child that outlived its request: pid=%s",
                       process.pid)
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        with contextlib.suppress(RuntimeError):  # loop closed: watcher still reaps
            asyncio.ensure_future(self._terminate(process))

    async def _spawn(self) -> asyncio.subprocess.Process:
        """Start one child. Split out so tests can hold a spawn open."""
        return await asyncio.create_subprocess_exec(
            *self._command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            # stderr is DEVNULL on purpose: the child is silent by
            # contract, and an unread pipe that fills would block it
            stderr=asyncio.subprocess.DEVNULL,
            env=_child_env(),
        )

    async def _retire(self, process, reason: str) -> None:
        """Drop a broken child (kill + reap) so the next attempt respawns."""
        logger.warning("retiring regex child pid=%s: %s", process.pid, reason)
        if self._process is process:
            self._process = None
        await self._terminate(process)

    async def _terminate(self, process) -> None:
        """terminate -> wait -> SIGKILL -> longer wait, then return.

        Both signals are waited on, so a normal teardown returning means the
        child has an exit status (the tests assert exactly that). The honest
        limit: if a wait itself gives up or is cancelled, the SIGKILL has
        already been delivered synchronously and asyncio's child watcher
        finishes the reap — "we stopped waiting" never means "the child is
        still running", it only means the exit status may land a moment later.

        Safe to call from a cancelled task: an interrupted grace wait still
        escalates to SIGKILL (which cannot be caught or ignored), and the
        cancellation is re-raised only once the child has been dealt with —
        so shutdown gets both its cancellation and its dead child.
        """
        cancellation: asyncio.CancelledError | None = None
        if process.returncode is None:
            cancellation = await self._signal_and_wait(process, process.terminate,
                                                       KILL_GRACE_S)
        if process.returncode is None:
            cancellation = await self._signal_and_wait(process, process.kill,
                                                       REAP_GRACE_S, cancellation)
        if process.stdin is not None:
            with contextlib.suppress(Exception):
                process.stdin.close()
        if cancellation is not None:
            raise cancellation

    async def _signal_and_wait(self, process, send, timeout: float, cancellation=None):
        """Send one signal and wait `timeout` for the reap.

        Returns a CancelledError captured from the wait instead of raising it,
        so _terminate can still escalate; a child that is already gone
        (ProcessLookupError) reports through process.returncode.
        """
        with contextlib.suppress(ProcessLookupError):
            send()
        try:
            await asyncio.wait_for(process.wait(), timeout)
        except TimeoutError:
            pass                      # the caller escalates to the next signal
        except asyncio.CancelledError as pending:
            return cancellation or pending
        return cancellation
