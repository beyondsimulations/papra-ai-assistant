"""One rule about the characters that must never reach a reader raw.

Two surfaces in this suite hand a human or an agent text that somebody else wrote: `papra-suite
events` prints rows of `ingest_events` (a filename from a MIME part, a sender from a header, an
upstream error string), and the MCP document tools return the text an index extracted from a PDF
that arrived over SMTP. The rule is the same in both places, and it used to live twice — a category
set in `__main__` and whatever a tool author reached for next. That is the drift this module exists
to prevent, and the drift's symptom is specific: the same bytes rendering safely in one place and
acting in the other.

**What gets removed, and what does not.** `UNSAFE_CATEGORIES` is the categories, copied from the
CLI's own reasoning: C0 and C1 controls (including ESC `0x1b` and C1 CSI `0x9b`, which are active
escape sequences — a stored `\\x1b[2Jevil.pdf` contains no whitespace at all, and printed raw its
bytes drive the terminal that was supposed to be displaying them), format characters (the zero-width
splices that join two values into one visually, and the directional overrides like U+202E which
reorder what a reader sees), surrogates, private-use and unassigned code points, and the Unicode
line separators U+2028/U+2029, which some renderers break a line on and so can still forge a row.

Newline, carriage return and tab are kept, and that is the whole difference between this rule and
`store.sanitize_event_text`. That one exists to keep one ingest event on one line, so it turns every
control character into a space and collapses whitespace — right for a column of a database row a
CLI prints, wrong for a document body, where a paragraph break is content and "Rechnung\\n4711"
turned into "Rechnung 4711" is a different transcript. Each is the honest rule for its own medium;
neither is the rule for the other.

`isprintable()` deliberately does **not** decide removal here. It is what the CLI uses to decide
whether a field needs quoting at all, and there a non-printable becomes a visible escape, so nothing
is lost. Applied to a body it would delete U+00A0, a *space* that Python calls non-printable, and
merge `mot\\u00a0entier` into `motentier`. Merging two words to defend against a character that
cannot act on anything is its own corruption, so the category filter decides, and the kept controls
are named.
"""

from __future__ import annotations

import unicodedata

#: Categories that must never reach a reader raw. The single definition: `papra_suite.__main__`
#: imports this same object for its terminal quoting, so the CLI and the MCP tools cannot disagree
#: about what "unsafe" means.
UNSAFE_CATEGORIES = frozenset({"Cc", "Cf", "Cs", "Co", "Cn", "Zl", "Zp"})

#: The controls that are structure rather than spoofing. Everything else in `Cc` goes, including the
#: escape byte: what a document needs is its line breaks, not its terminal sequences.
KEPT_CONTROLS = frozenset("\n\r\t")


def is_unsafe_char(char: str) -> bool:
    """Whether one character must not be handed to a reader.

    Length-1 string in, decision out; see the module docstring for why `isprintable()` is not the
    test and why newline survives.
    """
    if char in KEPT_CONTROLS:
        return False
    return unicodedata.category(char) in UNSAFE_CATEGORIES


def has_unsafe_char(text: str) -> bool:
    """Whether `strip_unsafe_text` would change this text.

    A separate predicate rather than `strip_unsafe_text(text) != text` because a caller reporting
    "this tool changed the bytes it was served" should say so from the same rule, and a document
    whose unsafe characters were already absent is reported as what it is: untouched.
    """
    return any(is_unsafe_char(char) for char in text)


def strip_unsafe_text(text: str) -> str:
    """`text` without the characters in `UNSAFE_CATEGORIES`, other than the kept controls.

    Removal rather than escaping, because the consumer here is a reading agent inside a context
    window: an escaped `\\u202e` in a document body is text the agent may quote onward, and a
    backslash-escape sequence is itself a small instruction-shaped thing to put in front of it. What
    is left is what the document said, minus the bytes that could act on a renderer.
    """
    if not has_unsafe_char(text):
        return text
    return "".join(char for char in text if not is_unsafe_char(char))
