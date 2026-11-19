"""The shipped runbook teaches commands, not SQLite incantations.

Why this is a test and not a convention: every one of these handovers started as the useful thing
to write down. "Run `sqlite3 "$DATA_DIR/state.db" "SELECT kind, status, count(*) FROM jobs
GROUP BY 1, 2;`" is short, it works today, and it is what the operator at 23:00 will type. Then the
statuses change, or the table does, or the command that already answers the question ships — and the
sentence keeps working just well enough to be typed while meaning something else. Task 7 pinned the
neighbouring failure (a runbook that cited a subcommand the build did not have) and Task 15's
`--drain` fired the tripwire at `test_pipeline.py:364-375` by shipping the command that message had
been hand-quoting SQL for.

What that review found, and what this file closes, is that the citation test on the other side
could not catch it: regexing `papra-suite <cmd> --flag` proves a printed command is *valid*, so a
stale-but-valid reading of the same table passes forever. So the rule here is the negative one, and
it is stated for the whole class rather than for the three comments that happened to be caught this
round: no shipped file hands the operator a raw query, and no shipped file but the store contains
SQL.
"""

import re
from pathlib import Path

import pytest

import papra_suite

PACKAGE_DIR = Path(papra_suite.__file__).resolve().parent
README = PACKAGE_DIR.parent / "README.md"

#: A read handed to a human to run by hand: the `sqlite3` CLI and a `SELECT` on one line. Written to
#: catch the shape rather than the three strings it replaced, so any database filename and any query
#: is caught, and a positive control below proves the pattern is not vacuous.
RAW_SQL_HANDOVER = re.compile(r"sqlite3\b[^\n]*\bSELECT\b", re.IGNORECASE)

#: The one raw statement the runbook still shows, and why it stays: deleting leftover `title` rows
#: has no command equivalent — `jobs` reads, resets one row, and gates. Enumerated here so the
#: exception is one line, is checked to still be a DELETE, and cannot grow into a read.
ACCEPTED_RAW_WRITES = ("DELETE FROM jobs",)

#: The CLI named beside its database file, options and a backslash continuation allowed between
#: them: that pair is what "run this" looks like when it is written down.
_RAW_INSTRUCTION = re.compile(
    r"""\bsqlite3\b(?:\s+-{1,2}\S+)*\s*["'`]?[^\s"'`]*\.(?:db|sqlite\d*)""", re.IGNORECASE)

#: A markdown code fence, which is the runbook's way of saying "this is what you type".
_FENCE = re.compile(r"^\s*```")
_SQLITE_MENTION = re.compile(r"\bsqlite3\b", re.IGNORECASE)

#: The runbook section that turns the listener on, named by its heading.
MCP_SECTION_HEADING = "### Publishing the index to an agent"


def sole_handover(text, *, name="README.md"):
    """The rule itself, as a function: one raw handover in `text`, and it is the accepted write.

    Raised as a function rather than inline in the test so a canary below can point it at a README
    with a handover injected — the mutation "let the guard accept any number of raw invocations" has
    to break something, and a test that only reads a clean tree cannot tell a strict guard from a
    blind one.
    """
    offenders = raw_invocations(text, name=name)
    assert len(offenders) == 1, f"expected one raw handover in {name}, got {offenders}"
    # The statement continues on the next line of the code block, so the exception is checked
    # against the whole incantation rather than against the line the CLI name happens to sit on.
    number = offenders[0][0]
    body = " ".join(text.splitlines()[number - 1:number + 2])
    assert any(write in body for write in ACCEPTED_RAW_WRITES), body
    return offenders[0]


def _lines_with_context(text):
    """`(line_number, text, inside_a_fence)` triples, with backslash continuations joined.

    `sqlite3 \\` on one line and the database on the next are one command: a guard that reads only
    physical lines would call half of it prose. The number reported is the command's first line,
    which is the line an operator copies.
    """
    marks = []
    inside = False
    for index, line in enumerate(text.splitlines()):
        opening = bool(_FENCE.match(line))
        marks.append((index + 1, line, inside, line.rstrip().endswith("\\")))
        if opening:
            inside = not inside

    joined = []
    pending = None
    for number, line, in_fence, continues in marks:
        if pending is None:
            pending = [number, line, in_fence]
        else:
            pending[1] += " " + line.strip()
        if not continues:
            joined.append((pending[0], pending[1], pending[2]))
            pending = None
    if pending is not None:
        joined.append((pending[0], pending[1], pending[2]))
    return joined


def raw_invocations(text, *, name="README.md"):
    """Every line of `text` that *hands an operator a SQLite statement*, with its line number.

    **The distinction is the whole of it, so it is stated here.** An *instruction to run* raw SQL is
    what the rule forbids; prose that merely mentions `sqlite3` is not, and it would be a worse
    document if it pretended otherwise — the runbook has good reasons to say things like "not
    a SQLite incantation at 23:00", and a test that edited English instead of handovers would be
    quietly switched off the next time somebody needed three `sqlite3` mentions in one paragraph.
    Two shapes count as instructions:

    * the CLI named beside its database file, so `sqlite3 "$DATA_DIR/state.db" "INSERT ..."` — with
      options (`sqlite3 -readonly state.db …`) and a backslash continuation allowed between them, so
      the shape is caught rather than the three strings it replaced; and
    * any `sqlite3` inside a fenced code block, because a fence needs no verb to be something a
      person pastes: it is what an operator types.

    `name` is the file an offender is reported against; the scan is over the text, so a canary
    below can inject an invocation into a copy of a shipped file and see it named.
    """
    found = []
    for number, line, in_fence in _lines_with_context(text):
        instruction = bool(_RAW_INSTRUCTION.search(line)) or bool(
            in_fence and _SQLITE_MENTION.search(line))
        if instruction:
            found.append((number, f"{name}:{number}: {line.strip()[:90]}"))
    return found


def mcp_section(text):
    """The runbook's MCP section, heading to next heading — the scope the enablement rule reads."""
    lines = text.splitlines()
    start = next((index for index, line in enumerate(lines)
                  if line.strip() == MCP_SECTION_HEADING), None)
    if start is None:
        raise AssertionError(f"the runbook lost its {MCP_SECTION_HEADING!r} section")
    end = next((index for index in range(start + 1, len(lines))
                if lines[index].startswith("### ")), len(lines))
    return "\n".join(lines[start:end])


def _shipped_sources():
    """Every Python file in the package, plus the README an operator follows."""
    return sorted(PACKAGE_DIR.rglob("*.py")), README


def test_no_shipped_file_hands_the_operator_a_raw_sql_query():
    """No `sqlite3 … SELECT` in any shipped source file or in the README.

    The scan is asserted non-empty, because a test that matched no files and a clean tree look the
    same from here. The positive control is asserted first, because a regex that stopped matching is
    exactly how this class of test goes quietly green.
    """
    assert RAW_SQL_HANDOVER.search(
        'sqlite3 "$DATA_DIR/state.db" "SELECT count(*) FROM jobs WHERE kind=\'title\'"'
    ), "the handover pattern matches nothing, so this test would prove nothing"

    sources, readme = _shipped_sources()
    assert len(sources) > 10 and readme.is_file(), "the scan found nothing to scan"

    offenders = []
    for path in [*sources, readme]:
        text = path.read_text(encoding="utf-8")
        for number, line in enumerate(text.splitlines(), start=1):
            if RAW_SQL_HANDOVER.search(line):
                offenders.append(f"{path.name}:{number}: {line.strip()[:90]}")

    assert not offenders, "raw SQL handed to an operator:\n" + "\n".join(offenders)


def test_the_only_raw_statement_left_is_the_one_with_no_command():
    """One raw invocation is handed over, it is the `DELETE`, and the classifier tells the two
    shapes of `sqlite3` apart in both directions.

    A `DELETE` of leftover rows is tidiness, not a reading, and no subcommand does it, so it is
    the one place this rule makes an exception — and pinning it as the only one is what keeps the
    exception from becoming the habit. The pattern was refined for the MCP round (the runbook had
    gained two `INSERT OR REPLACE INTO settings` lines beside a paragraph conceding that raw SQL was
    not a thing to hand an operator at 2am), and the refinement is the instruction/prose distinction
    in `raw_invocations`, never the count: three handovers still fail, exactly as three did before.
    """
    # Positive control, both ways. A regex that stopped matching is how this class of test goes
    # quietly green, and a pattern that matched prose as well gets relaxed by whoever it annoys.
    for sample, expected in (
        ("sqlite3 \"$DATA_DIR/state.db\" \"INSERT OR REPLACE INTO settings(key, value) "
         "VALUES ('mcp_server_enabled', '1');\"", 1),
        ("```bash\nsqlite3 -readonly \"$DATA_DIR/state.db\" \"SELECT 1\"\n```", 1),
        ("```bash\nsqlite3 \\\n  \"$DATA_DIR/state.db\" \\\n  \"DELETE FROM jobs\"\n```", 1),
        ("The only way in is `sqlite3`, which is not a thing to hand an operator at 2am.", 0),
        ("import sqlite3\n\nexcept sqlite3.Error as error:\n    pass\n", 0),
    ):
        assert len(raw_invocations(sample, name="<control>")) == expected, sample

    readme = README.read_text(encoding="utf-8")
    # Canary, README: a second write invocation — the exact line the MCP round shipped — is caught.
    extra = ('\n```bash\nsqlite3 "$DATA_DIR/state.db" "INSERT OR REPLACE INTO settings(key, '
             "value) VALUES ('mcp_bind_nonloopback', '1');\"\n```\n")
    assert len(raw_invocations(readme + extra)) == len(raw_invocations(readme)) + 1, \
        "the guard no longer notices a handover being added to the runbook"
    # The same injection, aimed at the rule rather than the pattern: a guard relaxed to accept any
    # number of raw invocations stops raising here, and that is what this assertion is for.
    with pytest.raises(AssertionError, match="one raw handover"):
        sole_handover(readme + extra)
    # And prose still does not count, or the refinement would just be a mention-counter.
    assert raw_invocations(readme + "\nThe row is prose about `sqlite3`, not a command.\n") == \
        raw_invocations(readme)

    sole_handover(readme)

    # Canary, package: a handover in a shipped module is named too, and by line.
    sources, _readme = _shipped_sources()
    shipped = []
    for path in sources:
        shipped += raw_invocations(path.read_text(encoding="utf-8"), name=path.name)
    assert not shipped, "raw SQL handed to an operator from a module:\n" + "\n".join(shipped)
    module = (PACKAGE_DIR / "mcp_cli.py").read_text(encoding="utf-8")
    injected = raw_invocations(module + '\n# sqlite3 "$DATA_DIR/state.db" "DELETE FROM settings"\n',
                               name="mcp_cli.py")
    assert len(injected) == 1, injected


def test_the_mcp_section_names_the_command_that_sets_the_two_rows():
    """Where the runbook asks "does the listener run?", it has to name the command.

    The positive half of a rule the guard above enforces only negatively — and the half that made
    this a product gap rather than a wording problem: two settings rows gated the listener, the
    section documented them as SQLite, and `PAPRA_MCP_HOST` alone was a refusal. The section must
    name `papra-suite mcp` and hand over no `settings` row to type by hand.
    """
    section = mcp_section(README.read_text(encoding="utf-8"))

    assert "papra-suite mcp --enable" in section, "the MCP section lost the command that enables"
    assert "papra-suite mcp --allow-nonloopback" in section
    assert not raw_invocations(section, name="mcp section")
    assert "INSERT OR REPLACE INTO settings" not in section


def test_sql_lives_in_the_store_and_nowhere_else():
    """No shipped module but `store.py` names a table.

    This is the same rule seen from the other side: the reason a docstring can go stale about the
    schema is that it contains the schema. A module that quotes `FROM jobs` has taken a copy of the
    queue's shape, and the copy is what an operator will be told to run.
    """
    sources, _readme = _shipped_sources()
    offenders = [path.name for path in sources
                 if path.name != "store.py" and "FROM jobs" in path.read_text(encoding="utf-8")]

    assert not offenders, f"modules naming the jobs table directly: {offenders}"


def test_the_handover_sites_cite_the_drain_command_that_proves_quiescence():
    """Where the runbook needs "is that queue empty?", it names the gate.

    Each site is a different handover step, so each names the kind that proves *its* step: the
    title loop for the metadata writer, `ocr` for the content writer, `index` for the rebuild's
    finish line. Naming the wrong kind would be the same class of defect in a better-looking costume
    — a command that parses, and proves the wrong thing.
    """
    pipeline = (PACKAGE_DIR / "pipeline.py").read_text(encoding="utf-8")
    mail = (PACKAGE_DIR / "mail.py").read_text(encoding="utf-8")
    readme = README.read_text(encoding="utf-8")

    assert "papra-suite jobs --kind title --drain" in pipeline, "the title handover lost its gate"
    assert "papra-suite jobs --kind ocr --drain" in pipeline, "the content handover lost its gate"
    assert "papra-suite jobs --kind ocr --drain" in mail, "the intake comment lost its gate"
    assert "papra-suite jobs --kind index --drain" in readme, "the rebuild lost its finish line"
