"""`papra-suite metadata` — the operator's door to Papra's `name`, `documentDate`, `notes`.

Two commands wear one subcommand, and they are opposites. The delivery pass reads Kreuzakt
and writes Papra, which means it needs both services and refuses when the gate is shut.
`--restore-auto` touches only this suite's ledger, which means it works with Papra down,
Kreuzakt down, and the gate shut — deliberately, because it is the command an operator
reaches for during the incident, when the last thing they need is a network dependency in
the escape hatch.

Why the escape hatch has to exist at all: `deliver_metadata` protects a field permanently
the moment it finds a value there that is not ours, and protection is meant to be
permanent. That is the correct default for a human's rename — the alternative is a nightly
job that quietly puts `papra-doc-1-a1b2c3d4` back — but it needs a way back that is a
deliberate command rather than a SQLite incantation, and `ledger_clear_protection` is the
only writer of those flags besides the delivery itself.

`--force` is not the way back. It overrides protection for one process and leaves the flags
set, which is what makes it safe to reach for when the question is "is the value in the
index right after all?" rather than "stop protecting this field forever". It prints the
plan and exits `1` unless `--yes-i-mean-it` is present, because the difference between
those two is exactly the thing an operator gets wrong at 23:00.

`--dry-run` is on both, and it is the reason `--restore-auto` reports counts per field
rather than as one number: a rehearsal that reports a different scope than the real run
would cover is worse than no rehearsal, and per-field counts are what both commands can
state exactly without inventing a query that guesses at the union.

One thing it is not: a preview through a shut gate. The delivery pass checks
`index_metadata_delivery` before it walks anything and exits `2` when the flag is off, so the
pre-flight this command offers is the one you run *after* the flip, with the flag on, in a
test deployment if the real one is not ready — `check` is what answers the earlier question,
"who owns these fields today". Putting `dry_run` into the gate test instead would let a
flag-off process read every document in the corpus, which is the door the gate exists to shut
and the one property that made the `OCR_LANGUAGES` mistake invisible.
"""

from __future__ import annotations

import asyncio
import sys

from papra_suite.indexing import (
    METADATA_FIELDS,
    METADATA_PROTECTED,
    METADATA_WRITTEN,
    DeliveryDisabled,
    IndexFailed,
    deliver_metadata,
)
from papra_suite.kreuzakt_client import KreuzaktClient, KreuzaktError
from papra_suite.papra_client import PapraClient, PapraError
from papra_suite.store import Store

#: Exit codes, matching what `check` and `replay` already mean by them: `0` the command did
#: what was asked, `1` it stopped partway or refused an unconfirmed write, `2` it would not
#: start at all (bad scope, gate off, unusable store).
EXIT_OK = 0
EXIT_INCOMPLETE = 1
EXIT_REFUSED = 2

#: How many references one page of the walk reads. `ledger_delivered_refs` bounds it for
#: us; the CLI keeps the pages moving until a page comes back short.
PAGE_SIZE = 200


def add_subparser(subparsers) -> None:
    """Register `metadata` on the entry point's subparser table."""
    parser = subparsers.add_parser(
        "metadata",
        help="deliver Kreuzakt's title, date and note into Papra, or restore a "
             "protected field to automatic delivery (--restore-auto)",
    )
    intent = parser.add_mutually_exclusive_group()
    intent.add_argument(
        "--restore-auto", action="store_true",
        help="clear protection in scope so delivery writes these fields again. Ledger-only: "
             "needs neither Papra nor Kreuzakt, and works while the delivery gate is shut",
    )
    intent.add_argument(
        "--force", action="store_true",
        help="deliver once regardless of protection. Overrides this run, not the flags: "
             "run --restore-auto after it if the field should stay automatic. Needs "
             "--yes-i-mean-it",
    )
    parser.add_argument("--org-id", default=None,
                        help="only this Papra organization (default: every organization "
                             "this suite knows)")
    parser.add_argument("--document-id", default=None,
                        help="only this Papra document")
    parser.add_argument("--field", choices=METADATA_FIELDS, default=None,
                        help="only this field (default: name, date and notes). `content` is "
                             "not a metadata field and has its own flag")
    parser.add_argument("--dry-run", action="store_true",
                        help="report the verdicts and change nothing, not even protection")
    parser.add_argument("--yes-i-mean-it", action="store_true",
                        help="confirm that --force may overwrite values a human wrote")


def run_metadata(config, args, *, store: Store | None = None, papra=None,
                 kreuzakt=None) -> int:
    """Do what was asked, and say what it cost. Returns the process exit code.

    `store`, `papra` and `kreuzakt` are injectable for the same reason `verify_once` takes
    its clients: the interesting behaviour here is what the tool does when one of the three
    is unreachable, and that is not something a test should stage by unplugging a container.
    """
    owns_store = store is None
    if owns_store:
        config.data_dir.mkdir(parents=True, exist_ok=True)
        store = Store(config.data_dir / "state.db")
    try:
        store.migrate()
        if args.restore_auto:
            return _restore_auto(store, args)
        return _deliver(config, args, store, papra, kreuzakt)
    except ValueError as error:
        # `get_generation` and the store's own integrity refusals arrive here. They mean
        # "this store will not answer the question", which is a different report from "I
        # walked the corpus and one row did not read", and it must not be counted as one.
        print(f"metadata refused: {error}", file=sys.stderr)
        return EXIT_REFUSED
    finally:
        if owns_store:
            store.close()


def _scope_label(args) -> str:
    if args.document_id is not None:
        return f"document {args.document_id}" + (
            f" in {args.org_id}" if args.org_id else "")
    return f"organization {args.org_id}" if args.org_id else "every organization"


def _fields_label(field: str | None) -> str:
    return field if field else ", ".join(METADATA_FIELDS)


def _restore_auto(store: Store, args) -> int:
    """Hand the fields back to automatic delivery, in scope.

    Ledger-only by construction: `ledger_clear_protection` is the single writer of those
    flags, so this cannot leave Papra holding a value the ledger forgot. That is also why
    the count is the store's rather than something this function computes — the store knows
    which rows the UPDATE actually matched, and "references changed" is the only number that
    tells an operator whether a repeat of the command did anything.
    """
    if args.dry_run:
        print(f"dry run: {_protected_line(store, args)} in {_scope_label(args)} "
              f"[fields: {_fields_label(args.field)}]")
        return EXIT_OK
    cleared = store.ledger_clear_protection(
        field=args.field, org_id=args.org_id, document_id=args.document_id)
    print(f"restored automatic delivery on {cleared} reference(s) "
          f"[fields: {_fields_label(args.field)}; {_scope_label(args)}]")
    if cleared:
        # Said out loud because the command's name promises more than it does. Un-protecting
        # re-admits the field to the ownership question; it does not answer it, and a value we
        # never wrote gets protected again on the next pass — which reads like the command did
        # nothing unless it is explained here, at the moment the operator is holding it.
        print("the fields are back in the ownership question, which is not the same as being "
              "ours: where Papra still holds a value this bridge never wrote, the next "
              "delivery protects the field again. To put the index's value back and keep it "
              "automatic, run the same scope with --force --yes-i-mean-it (which overwrites "
              "the human's value and leaves protection set), then --restore-auto once more")
    return EXIT_OK


def _protected_line(store: Store, args) -> str:
    """How many references hold each protection flag, in scope, without changing anything.

    Per field, not as one "protected" total, and not as a distinct-reference count. The two
    commands must not disagree about the scope: `ledger_clear_protection` matches every row
    in scope that has any in-scope flag set, so a dry run that walked a narrower population
    — the references bound to the current generation, say — would read "0 protected" and
    then un-protect three. Per-field counts are what `ledger_stats` and `ledger_get` can
    both answer exactly, for any scope, with no new query and no deleted-row discrepancy.
    """
    if args.document_id is not None:
        ref = store.ledger_get(args.org_id or _only_org(store), args.document_id)
        if ref is None:
            return "no reference"
        counts = {field: int(getattr(ref, f"{field}_protected")) for field in METADATA_FIELDS}
    else:
        org_ids = [args.org_id] if args.org_id else [
            org.papra_org_id for org in store.list_orgs()]
        stats = store.ledger_stats(org_ids=org_ids)
        counts = {field: stats[f"protected_{field}"] for field in METADATA_FIELDS}
    return ", ".join(f"{counts[field]} {field}" for field in METADATA_FIELDS)


def _only_org(store: Store) -> str:
    """The organization to assume when `--document-id` was given alone.

    One bucket is the whole deployment for most installations, so a lone `--document-id` is
    not ambiguous there; with several buckets it is, and guessing would look up the document
    in the wrong one and report "no reference" for a document that exists.
    """
    orgs = [org.papra_org_id for org in store.list_orgs()]
    if len(orgs) == 1:
        return orgs[0]
    raise ValueError(
        "--document-id needs --org-id when this suite knows more than one organization "
        f"({', '.join(orgs) or 'none'}); the document would be looked up in the wrong bucket")


def _deliver(config, args, store: Store, papra, kreuzakt) -> int:
    """Walk the scope, one PATCH per reference at most, and report every verdict."""
    if args.force and not args.yes_i_mean_it:
        print(f"would deliver [{_fields_label(args.field)}] in {_scope_label(args)}, "
              "overwriting values a human wrote")
        print("not run: --force overwrites protection, which is the one thing here that "
              "destroys a user's text. Add --yes-i-mean-it, or use --dry-run to see the "
              "verdicts and --restore-auto to lift the protection deliberately")
        return EXIT_INCOMPLETE

    if not store.settings_flag("index_metadata_delivery"):
        # Named the variable, because whoever types this command is the person who just set
        # it and concluded it did nothing.
        print("metadata refused: PAPRA_INDEX_METADATA_DELIVERY is off, so this bridge does "
              "not write name, documentDate or notes. Set it and restart, or run "
              "`papra-suite check` to see who owns those fields today", file=sys.stderr)
        return EXIT_REFUSED

    generation = store.get_generation()
    refs, problem = _refs_in_scope(store, args, generation)
    if problem is not None:
        print(f"metadata refused: {problem}", file=sys.stderr)
        return EXIT_REFUSED
    if not refs:
        print(f"nothing to deliver: no indexed reference in {_scope_label(args)}")
        return EXIT_OK

    fields = (args.field,) if args.field else None
    return asyncio.run(_walk(store, refs, fields=fields, force=args.force,
                             dry_run=args.dry_run, config=config, papra=papra,
                             kreuzakt=kreuzakt))


def _refs_in_scope(store: Store, args, generation: int) -> tuple[list, str | None]:
    """The references this command may touch, or the reason it cannot name any.

    Scope is `ledger_delivered_refs`: bound to the live generation, and not soft-deleted.
    Its name says "delivered" and its filter does not mean "we wrote this one" — see that
    method — which is exactly the population wanted here, because the references that were
    never delivered are the ones whose titles are still wrong.

    A row with no `kreuzakt_id` could not be delivered from even in principle: there is no
    index row to read a title out of, so it is `ledger_needs_index`'s work, not this
    command's. A reference bound under an older generation points at an index document a
    rebuild has already orphaned, so writing from it would deliver stale metadata.

    `--document-id` bypasses the paging but not those filters, and says which one excluded
    the row: "no reference" for a document the operator can see in the UI is a report they
    cannot act on.
    """
    if args.document_id is not None:
        org_id = args.org_id or _only_org(store)
        ref = store.ledger_get(org_id, args.document_id)
        if ref is None:
            return [], (f"no ledger reference for {org_id}/{args.document_id}: this suite "
                        "has never indexed that document, so it has nothing to deliver "
                        "from")
        if ref.deleted_at is not None:
            return [], (f"{org_id}/{args.document_id} is marked deleted in the ledger "
                        f"(deleted_at={ref.deleted_at}): the delete pass owns that document")
        if ref.kreuzakt_id is None:
            return [], (f"{org_id}/{args.document_id} is not bound to a Kreuzakt document "
                        "yet: index it first")
        if ref.delivered_generation != generation:
            return [], (f"{org_id}/{args.document_id} is bound under generation "
                        f"{ref.delivered_generation}, not the current {generation}: stale "
                        "backlog, not metadata to deliver")
        return [ref], None

    if args.org_id and store.get_org_by_papra_id(args.org_id) is None:
        # Looked up by Papra's id, not this suite's internal uuid: `--org-id` has to be the
        # string the operator can read off the Papra UI or an API response, and the ledger's
        # own column holds that same value.
        known = ", ".join(org.papra_org_id for org in store.list_orgs()) or "none"
        return [], f"unknown organization {args.org_id}; known: {known}"
    org_ids = [args.org_id] if args.org_id else None
    refs: list = []
    after: tuple[str, str] | None = None
    while True:
        page = store.ledger_delivered_refs(generation=generation, after=after,
                                           org_ids=org_ids, limit=PAGE_SIZE)
        refs.extend(page)
        if len(page) < PAGE_SIZE:
            return refs, None
        last = page[-1]
        after = (last.papra_org_id, last.papra_document_id)


async def _walk(store: Store, refs, *, fields, force: bool, dry_run: bool, config,
                papra, kreuzakt) -> int:
    """One reference at a time, one line each: the walk is the point of the command.

    A reference whose index row will not answer is counted and skipped rather than fatal.
    The convergence pass learned this on a live corpus: one unreadable document must not
    cost the operator the rest of the run, because the retry meets the same wall at the same
    place and the command becomes something to run in the middle of the night in pieces.

    Clients this function created are closed by this function, inside the loop that used
    them: a transport belongs to the event loop its connections were made on, and
    `asyncio.run` has already closed that loop by the time `run_metadata` returns. Injected
    clients stay the caller's, which is what lets a test inject one broken client and two
    working ones.

    A gate that closes mid-walk is different from an unreadable row and stops the run: the
    flag is one setting, so every remaining reference answers the same way, and the honest
    report is "stopped here" with what came before it, not one line per document.
    """
    own_clients = papra is None or kreuzakt is None
    if own_clients:
        papra = PapraClient(config.papra_base_url, config.papra_api_key)
        kreuzakt = KreuzaktClient(config.kreuzakt_base_url)
    written = 0
    protected = 0
    overrode = 0
    unreadable = 0
    refused = 0
    walked = 0
    gate_closed = ""
    try:
        for ref in refs:
            # The flags as they stand before this call, so "how many human values did this
            # run overwrite?" is answered from evidence rather than from the force flag: a
            # forced run that found nothing protected overwrote nothing, and saying
            # otherwise would teach the operator to distrust the number that matters.
            before = {field: bool(getattr(ref, f"{field}_protected"))
                      for field in METADATA_FIELDS}
            try:
                kz_doc = await kreuzakt.get_document(ref.kreuzakt_id)
            except KreuzaktError as error:
                unreadable += 1
                print(f"{ref.papra_org_id}/{ref.papra_document_id}  unreadable index row "
                      f"{ref.kreuzakt_id}: {error}")
                continue
            try:
                result = await deliver_metadata(papra, store, ref, kz_doc, fields=fields,
                                                force=force, dry_run=dry_run)
            except (PapraError, IndexFailed) as error:
                # Papra said no, or said the document is in the trash. Counted apart from an
                # unreadable index row because the two send an operator to different
                # services, and both counted apart from `protected` because a refusal from
                # Papra is not a human owning a field.
                refused += 1
                print(f"{ref.papra_org_id}/{ref.papra_document_id}  not written: {error}")
                continue
            except DeliveryDisabled as error:
                # The gate closed under the walk: another process flipped the flag, or the
                # settings row changed underneath it. Every remaining reference answers
                # identically, so continuing would spend one line per document and stop
                # nowhere, and letting it escape would cost the operator the report of what
                # this run already wrote — which after a partial `--force` is the whole point
                # of the command. So: stop, and say exactly where.
                gate_closed = str(error)
                break
            actions = {field: _action(result, field) for field in METADATA_FIELDS}
            written += sum(1 for action in actions.values()
                           if action == METADATA_WRITTEN)
            protected += sum(1 for action in actions.values()
                             if action == METADATA_PROTECTED)
            overrode += any(before[field] and actions[field] == METADATA_WRITTEN
                            for field in METADATA_FIELDS)
            print(_line(ref, actions, fields))
            walked += 1          # completed, so a stop can report how far the run actually got
    finally:
        if own_clients:
            await papra.aclose()
            await kreuzakt.aclose()

    summary = (f"{len(refs)} references, {written} fields written, {protected} protected")
    if overrode:
        summary += f", overwrote protected values on {overrode} reference(s)"
    if unreadable:
        summary += f", {unreadable} unreadable"
    if refused:
        summary += f", {refused} refused by papra"
    if gate_closed:
        summary += (f", gate closed after {walked} of {len(refs)} references; the rest were "
                    "not attempted")
    print(("dry run: " if dry_run else "") + summary)
    if dry_run and protected:
        print("protected fields are a human's; `papra-suite metadata --restore-auto "
              "[--field F]` hands them back to automatic delivery")
    return EXIT_INCOMPLETE if (unreadable or refused or gate_closed) else EXIT_OK


def _action(result, field: str) -> str:
    """One field's verdict out of a `MetadataResult`, by the field's own name."""
    return getattr(result, f"{field}_action")


def _line(ref, actions: dict, fields) -> str:
    """One reference's verdicts, with the fields outside the scope left off.

    Left off rather than printed as `skipped`: `skipped` is a verdict with a meaning — the
    index has nothing to say about that field — and printing it for a field the operator
    deliberately excluded would put a claim on the line they are reading that was never
    checked.
    """
    shown = METADATA_FIELDS if fields is None else tuple(fields)
    verdicts = "  ".join(f"{field}={actions[field]}" for field in shown)
    return f"{ref.papra_org_id}/{ref.papra_document_id}  {verdicts}"
