"""Child-process entry point: runs the operator's regex so it can be killed.

Started by papra_suite.safematch as `python -m papra_suite.regex_worker`.

WHY THIS EXISTS: re.search() holds the GIL for its entire runtime, so a
catastrophic backtracking pattern cannot be interrupted from inside the
service process — no asyncio timeout, thread, or signal handler can fire
until the search drains. A separate process can be SIGTERM/SIGKILLed by the
kernel at any moment, which is the bound the subject filter actually needs.

PROTOCOL (newline-delimited JSON, one request -> exactly one reply line):
    in:  {"id": int, "op": "search"|"validate", "pattern": str,
          "subject": str, "flags": int}
    out: {"id": int, "matched": bool}      search success
         {"id": int, "ok": true}           validate success
         {"id": int, "error": str}         any rejection or failure

HARD RULE: stdout IS the protocol channel. Nothing else may ever be written
to it — a stray print desynchronises (or, worse, silently mis-answers) the
parent, which treats an unparseable line as a dead child. So this module
installs no logging handlers, disables logging outright, and sends its own
diagnostics to stderr, where they stay quiet by default (the parent pipes
stderr to DEVNULL: an unread, full stderr pipe would block the child).
"""

import json
import logging
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
        _reply(stdout, _handle(request))


if __name__ == "__main__":
    raise SystemExit(main())
