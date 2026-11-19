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
    """The remaining `sqlite3` line is a write, and it is the only one.

    A `DELETE` of leftover rows is tidiness rather than a reading, and no subcommand does it, so it
    is the one place this rule makes an exception. Pinning it as the only one is what keeps the
    exception from becoming the habit.
    """
    lines = [(number, line) for number, line in
             enumerate(README.read_text(encoding="utf-8").splitlines(), start=1)
             if re.search(r"\bsqlite3\b", line)]

    assert len(lines) == 1, f"expected one raw handover in README, got {lines}"
    # The statement continues on the next line of the code block, so the exception is checked
    # against the whole incantation rather than against the line the CLI name happens to sit on.
    number = lines[0][0]
    body = " ".join(README.read_text(encoding="utf-8").splitlines()[number - 1:number + 2])
    assert any(write in body for write in ACCEPTED_RAW_WRITES), body


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
