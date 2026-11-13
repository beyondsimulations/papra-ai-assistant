"""Child-process entry point: runs the operator's regex so it can be killed.

Started by papra_suite.safematch as `python -m papra_suite.regex_worker`.

WHY THIS EXISTS: re.search() holds the GIL for its entire runtime, so a
catastrophic backtracking pattern cannot be interrupted from inside the
service process — no asyncio timeout, thread, or signal handler can fire
until the search drains. A separate process can be SIGTERM/SIGKILLed by the
kernel at any moment, which is the bound the subject filter actually needs.

PROTOCOL (newline-delimited JSON, one request -> exactly one reply line):
    in:  {"id": int, "op": "search"|"validate", "pattern": str,
          "subject": str, "flags": int, "deadline_s": float (optional)}
    out: {"id": int, "matched": bool}      search success
         {"id": int, "ok": true}           validate success
         {"id": int, "error": str}         any rejection or failure

WHY THE CHILD ALSO KILLS ITSELF. The parent's budget is enforced by the
parent, and the parent is not a trustworthy watchdog: SIGKILL or OOM it mid-
search and this process is an orphan at ppid=1 with nobody left to signal it
(measured: still pinning a core 100 s later, 98 CPU-seconds — and since the
cursor had not advanced, every restart re-armed another one). The self-check
between requests cannot help either, because a process blocked inside
re.search never returns to it. So each operation is wrapped in a wall-clock
self-deadline (deadline_s from the request; the parent owns the number) armed
with SIGALRM and NO Python handler: with no handler installed, SIGALRM's
default disposition terminates the process, which the kernel can do mid-
re.search where nothing this interpreter runs ever could. The alarm is
cancelled the moment an operation finishes, so an idle child — the normal
state, one child reused across a whole poll loop's life — is never killed
between requests; an idle orphan ends a different way, by stdin EOF in main().

TWO THINGS THIS IS NOT. It is not a child-lifetime bound: it bounds the ONE
operation that is running, and an idle child has no alarm pending. And it is
not portable: signal.alarm needs SIGALRM, so on a platform without it (Windows)
this module cannot bound itself at all and the parent's kill is all there is
(see _NO_SELF_DEADLINE). The whole-second granularity of signal.alarm also
makes this an order-of-magnitude bound, not a wall-clock promise.

HARD RULE: stdout IS the protocol channel. Nothing else may ever be written
to it — a stray print desynchronises (or, worse, silently mis-answers) the
parent, which treats an unparseable line as a dead child. So this module
installs no logging handlers, disables logging outright, and sends its own
diagnostics to stderr, where they stay quiet by default (the parent pipes
stderr to DEVNULL: an unread, full stderr pipe would block the child).
"""

import json
import logging
import math
import signal
import sys

# See the HARD RULE above: an imported module must not be able to write a
# log record anywhere near stdout, and stderr is discarded by the parent —
# every answer this process owes the parent travels through the reply line.
logging.disable(logging.CRITICAL)


def _reply(stream, payload: dict) -> None:
    """Write exactly one protocol line. ASCII-only, flushed, nothing else."""
    # ensure_ascii keeps the wire pure ASCII regardless of the operator's
    # pattern/subject (non-ASCII survives as \uXXXX escapes and decodes back
    # to the same string), so no locale/encoding setting can corrupt a reply.
    stream.write((json.dumps(payload) + "\n").encode("ascii", "backslashreplace"))
    stream.flush()


# A request with no deadline_s is a hand-made one (a foreign caller, a test, an
# older parent): serve it, but on this conservative bound rather than on none.
DEFAULT_DEADLINE_S = 300.0
_NO_SELF_DEADLINE = not hasattr(signal, "SIGALRM")   # e.g. Windows


def _self_deadline_s(request: dict) -> float:
    """The request's own bound, or DEFAULT_DEADLINE_S when it did not send one."""
    try:
        deadline = float(request.get("deadline_s"))
    except (TypeError, ValueError):
        return DEFAULT_DEADLINE_S
    if deadline != deadline or deadline <= 0:   # NaN, null, negative, zero
        return DEFAULT_DEADLINE_S
    return deadline


def _arm_self_deadline(seconds: float) -> None:
    """SIGALRM with no handler installed: the default disposition kills us.

    Deliberately NO signal.signal() call anywhere in this module: a Python
    handler could not run while re.search holds the interpreter, and
    installing one would also replace the terminating disposition that makes
    this work. signal.alarm() has whole-second granularity, so round up.
    """
    if _NO_SELF_DEADLINE:
        return
    signal.alarm(max(1, math.ceil(seconds)))


def _disarm_self_deadline() -> None:
    """An idle child is not an overrun one: the bound covers ONE operation."""
    if _NO_SELF_DEADLINE:
        return
    signal.alarm(0)


def _handle(request: dict) -> dict:
    """Run one request and turn any outcome into a reply payload."""
    request_id = request.get("id")
    operation = request.get("op")
    pattern = request.get("pattern") or ""
    subject = request.get("subject") or ""
    try:
        flags = int(request.get("flags") or 0)
    except (TypeError, ValueError):
        flags = 0
    try:
        if operation == "search":
            import re

            return {"id": request_id, "matched": re.search(pattern, subject, flags) is not None}
        if operation == "validate":
            # imported lazily and locally: the search path must not pay for
            # papra_suite.mail's import chain (fitz et al.), and importing
            # mail here keeps the ratio gate a single source of truth
            from papra_suite.mail import validate_subject_filter

            validate_subject_filter(pattern)
            return {"id": request_id, "ok": True}
        raise ValueError(f"unknown op {operation!r}")
    except Exception as error:  # noqa: BLE001 - every failure owes the parent a reply
        return {"id": request_id, "error": f"{error}"}


def main() -> int:
    """Serve requests until stdin closes. Exits 0; the parent owns lifecycle."""
    stdin, stdout = sys.stdin.buffer, sys.stdout.buffer
    while True:
        line = stdin.readline()
        if not line:
            return 0  # parent gone: exit instead of spinning on a closed pipe
        if not line.strip():
            continue
        try:
            request = json.loads(line.decode("utf-8"))
            if not isinstance(request, dict):
                raise TypeError("request must be a JSON object")
        except (UnicodeDecodeError, ValueError, TypeError) as error:
            _reply(stdout, {"id": None, "error": f"malformed request: {error}"})
            continue
        _arm_self_deadline(_self_deadline_s(request))
        try:
            reply = _handle(request)
        finally:
            _disarm_self_deadline()   # only reached if the alarm did not fire
        _reply(stdout, reply)


if __name__ == "__main__":
    raise SystemExit(main())
