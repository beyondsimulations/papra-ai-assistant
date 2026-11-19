"""`papra-suite mcp-token` — the bearer tokens the MCP server authenticates with.

The token is the whole access control on that server. There is no OAuth: the deployment is a
loopback listener behind `tailscale serve` on a tailnet the operator treats as private, and the
spec's own conclusion is that a fixed bearer token is what a private MCP server normally uses.
That decision puts everything on this file, so it has exactly two jobs.

**Never hold the plaintext.** A token is generated, hashed with sha256, and stored under that
digest; the plaintext is returned once, to the terminal of whoever minted it. The lookup direction
is the same shape: hash the candidate, ask for that one key. That is why no prefix of a token can
authenticate and why a wrong token and a missing token are the same event to the server — there is
no scan over stored tokens, so there is no order in which one could fail before the other, and
nothing here can be compared against in constant time "by accident" later.

**Scoping is a boundary, not a filter.** A token carries a bucket list or the whole corpus. The
bucket ceiling is the spec's fan-out bound (8 organisations) enforced at the place a scope is
actually created, so a scope that could never be honoured is refused rather than stored.

A token row is JSON in `settings` under `mcp_token:<sha256hex>`, holding
`{"label", "org_ids", "scope_all", "created_at", "revoked_at"}`. Revocation keeps the row: the
credential stops working, and the record of there having been one survives to be listed.

Labels and bucket ids are text somebody types at a keyboard, and both are echoed back by
`--list`. They are bounded and screened here, at the write, rather than at every print site.
"""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from papra_suite.logging_setup import SECRET_REGISTRY
from papra_suite.store import Store, sanitize_event_text
from papra_suite.store import _now as _store_now

logger = logging.getLogger("papra_suite.mcp_tokens")

#: Settings-key prefix for one row per token. `Store.list_setting_keys(prefix=...)` enumerates it.
TOKEN_SETTING_PREFIX = "mcp_token:"

#: Settings-key prefix for the one write the MCP request path may make: when this token last
#: authenticated. A separate family rather than a sixth key in the token row, because `_parse`
#: refuses a row whose key set is not exactly the five it knows — a new field in the row would stop
#: every credential minted before this change from authenticating. That is the authorisation
#: surface;
#: this is a report. The prefixes differ at the eleventh character (`_` against `:`), so the token
#: enumeration cannot pick a stamp up as a credential.
TOKEN_USE_PREFIX = "mcp_token_use:"

#: One settings write per token per minute, however many requests that token carries. The listener
#: shares `serve`'s single-writer WAL database with the delivery writes and the sweep, so without
#: this the write is per request: the difference between a timestamp and a contention source.
TOKEN_USE_THROTTLE_S = 60.0

#: How long the stamp's own connection waits for the write lock. Short on purpose — this happens
#: while a client waits on a tool call, and a stale timestamp is worth nothing beside a stalled one.
TOKEN_USE_BUSY_TIMEOUT_MS = 1000

#: digest → monotonic reading of the last write. In-process, so a restart loses at most a minute on
#: a stamp nobody reads for a week, and bounded by the number of credentials: only a token that
#: resolved is ever remembered here (`stamp_token_use`).
_recent_stamps: dict[str, float] = {}

#: Bytes of entropy behind `secrets.token_urlsafe`, per the 2026-11-16 spec's Operations section.
TOKEN_ENTROPY_BYTES = 32

#: A bearer header longer than this was not minted here. Hashing an arbitrarily long header is a
#: free-CPU door, so the candidate is bounded before it is digested.
TOKEN_MAX_CHARS = 512

LABEL_MAX_CHARS = 64
ORG_ID_MAX_CHARS = 128

#: How much of a `mcp_token_use:` row is printed as the `last used` column. `_store_now()` writes an
#: ISO-8601 timestamp (25-32 characters); the bound is for a row that is not one, because a settings
#: value is hand-editable text and this column goes into a tab-separated table.
_LAST_USED_MAX_CHARS = 64

#: The spec's ceiling on organisation fan-out, enforced where a scope is created: a token naming a
#: ninth bucket is a scope the server could never honour, and "never silently use a subset" is the
#: rule that makes refusing it the only honest option.
MAX_ORG_IDS = 8

# : Exit codes, with the meaning `check`, `replay`, `metadata` and `ledger` give them: `0` the
# command
#: did what was asked, `1` it stopped partway, `2` it would not start at all.
EXIT_OK = 0
EXIT_INCOMPLETE = 1
EXIT_REFUSED = 2


class TokenError(ValueError):
    """A mint or a revoke that will not be attempted. Everything the CLI prints as a refusal."""


@dataclass(frozen=True)
class TokenContext:
    """What a presented token is allowed to see, resolved from the settings row behind it.

    `scope_all` and a non-empty `org_ids` are mutually exclusive by construction — `mint_token`
    refuses the combination and `_context` refuses a row that carries it — so a caller may branch on
    `scope_all` without wondering what the list means when it is set.
    """

    label: str
    org_ids: frozenset[str]
    scope_all: bool


@dataclass(frozen=True)
class TokenInfo:
    """One row of `mcp-token --list`. No credential on it: not the plaintext, not the digest.

    A listing that printed the first eight characters of the hash was a listing that handed out a
    prefix of the key the row is stored under, and eight hex characters is a third of a sha256's
    input space rather than a hint. The label is the operator's name for the device, and it is
    enough to revoke by.

    `last_used` is the newest stamp `stamp_token_use` left for this credential, or None when it has
    never authenticated. It answers the question an operator asks before a `--revoke`: is
    this thing in use? The stamp is throttled to one write per token per minute, so the reading can
    lag a live session by up to a minute — which is a different claim from a credential that has no
    stamp at all, and it is printed as `never`.
    """

    label: str
    org_ids: tuple[str, ...]
    scope_all: bool
    created_at: str
    revoked_at: str | None
    last_used: str | None


def _has_control_chars(text: str) -> bool:
    """C0/C1 controls and DEL, which is the same screen `config.py` applies to environment values.

    Tab and newline in particular: `--list` output is tab-separated, so a stored tab would forge a
    column, and a stored newline would forge a row.
    """
    return any(ord(char) < 32 or ord(char) == 127 for char in text)


def _require_text(value: object, field: str, *, limit: int) -> str:
    """A bounded, printable string, or a refusal naming the field and the limit."""
    if not isinstance(value, str):
        raise TokenError(f"{field} must be text")
    cleaned = value.strip()
    if not cleaned:
        raise TokenError(f"{field} must not be empty")
    if _has_control_chars(cleaned) or not cleaned.isprintable():
        raise TokenError(f"{field} must not contain control characters")
    if len(cleaned) > limit:
        raise TokenError(f"{field} is {len(cleaned)} characters; the length limit is {limit}")
    return cleaned


def _require_org_ids(org_ids: object) -> tuple[str, ...]:
    """The bucket list, deduplicated and sorted, or a refusal.

    Sorting is not cosmetics: the stored row is what `--list` prints and what a diff between two
    deployments compares, and `--org-id b --org-id a` on Monday must read the same as the reverse on
    Tuesday.
    """
    if org_ids is None:
        return ()
    if isinstance(org_ids, str) or not hasattr(org_ids, "__iter__"):
        raise TokenError("org ids must be a list of organisation ids")
    buckets = {_require_text(org, "an org id", limit=ORG_ID_MAX_CHARS) for org in org_ids}
    if len(buckets) > MAX_ORG_IDS:
        raise TokenError(f"a token may name at most {MAX_ORG_IDS} buckets, not {len(buckets)}")
    return tuple(sorted(buckets))


def _token_key(raw: str) -> str:
    return TOKEN_SETTING_PREFIX + hashlib.sha256(raw.encode()).hexdigest()


def unknown_buckets(store: Store, org_ids: object) -> list[str]:
    """Granted ids that name no bucket this suite has ever seen, sorted.

    The union of the two local listings — `ledger_org_ids()` and `documents_org_ids()` — is the
    widest set of buckets this archive can prove: the ledger alone cannot name a bucket whose
    documents are all unledgered.

    This is a *screening* question, not an authority. Papra owns the list of organisations, and the
    2026-11-16 spec asks for validation against it; asking Papra here was rejected because minting
    and revoking have to work while Papra is down — rotating a lost credential during the outage
    that made it lost is the case that matters — and because a network round trip does not belong
    inside the one command that must succeed offline. So the check catches the failure it can see
    cheaply (a mistyped id, a bucket that has never been ingested here) and says what it is: an
    id that exists in Papra but is not yet indexed here is a warning, and an id that names a real
    but unrelated bucket is out of reach. Both are stated in the README's MCP section.
    """
    known = set(store.ledger_org_ids()) | set(store.documents_org_ids())
    return sorted({org for org in (org_ids or []) if org not in known})


def _rows(store: Store) -> list[tuple[str, dict]]:
    """Every token row as (key, document), dropping the ones that are not token documents.

    A row under this prefix that does not parse is reported and skipped rather than served: the
    alternative is for a hand-edited or half-written row to become a credential, and for a row whose
    `org_ids` went missing to become a token that grants everything the JSON forgot to restrict.

    What is logged is a **count**. The key is `mcp_token:<sha256hex>`, and log lines go to stderr
    and into the `log_lines` ring buffer the dashboard serves; a digest is unrecoverable behind 256
    bits, but "no credential material in the logs" is only a rule if no line is ever allowed to
    carry a piece of one.
    """
    found: list[tuple[str, dict]] = []
    rejected = 0
    for key in store.list_setting_keys(prefix=TOKEN_SETTING_PREFIX):
        raw = store.get_setting(key)
        if raw is None:
            continue
        try:
            document = json.loads(raw)
        except ValueError:
            rejected += 1
            continue
        if _parse(document) is None:
            rejected += 1
            continue
        found.append((key, document))
    if rejected:
        logger.warning("mcp: %d settings row(s) under %s are not usable token rows and were "
                       "ignored", rejected, TOKEN_SETTING_PREFIX)
    return found


def _parse(document: object) -> dict | None:
    """A token document, or None when it is not one. Fail closed, in both directions."""
    if not isinstance(document, dict):
        return None
    if set(document) != {"label", "org_ids", "scope_all", "created_at", "revoked_at"}:
        return None
    label = document["label"]
    org_ids = document["org_ids"]
    if not isinstance(label, str) or not label or _has_control_chars(label):
        return None
    if not isinstance(org_ids, list) or any(not isinstance(o, str) or not o for o in org_ids):
        return None
    if not isinstance(document["scope_all"], bool):
        return None
    if not isinstance(document["created_at"], str) or not document["created_at"]:
        return None
    revoked = document["revoked_at"]
    if revoked is not None and (not isinstance(revoked, str) or not revoked):
        return None
    # The contradiction a hand-edited row produces: all buckets, and a list of them.
    if document["scope_all"] and org_ids:
        return None
    return document


def _active_by_label(store: Store, label: str) -> tuple[str, dict] | None:
    for key, document in _rows(store):
        if document["label"] == label and document["revoked_at"] is None:
            return key, document
    return None


def mint_token(store: Store, *, label: str, org_ids: list[str],
               scope_all: bool = False) -> str:
    """Create a token and return its plaintext — the only place it exists outside a client.

    A duplicate *active* label is refused: two live credentials with one name make
    `--revoke --label` ambiguous, and the operator's mental model of a token is the device it is on.
    A revoked label may be minted again, because a rotation that invents `pi-2`, `pi-3` is a
    rotation nobody can audit afterwards.
    """
    name = _require_text(label, "a token label", limit=LABEL_MAX_CHARS)
    buckets = _require_org_ids(org_ids)
    if scope_all and buckets:
        raise TokenError(
            "an all-buckets token grants every bucket, so it cannot also name org ids — "
            "pass --all-buckets or --org-id, not both")
    if _active_by_label(store, name) is not None:
        raise TokenError(
            f"a token labelled {name!r} already exists and is still active; "
            "run `papra-suite mcp-token --list`, then `--revoke --label "
            f"{name}` before minting a replacement")

    raw = secrets.token_urlsafe(TOKEN_ENTROPY_BYTES)
    # Registered with the suite's redaction filter the moment it exists: the CLI prints this value
    # once by design, and from here on the only way it should ever reach a log line is as
    # `[REDACTED]` — including lines written by libraries that log full URLs.
    SECRET_REGISTRY.add(raw)
    document = {"label": name, "org_ids": list(buckets), "scope_all": bool(scope_all),
                "created_at": _store_now(), "revoked_at": None}
    store.set_setting(_token_key(raw), json.dumps(document, sort_keys=True))
    # The plaintext is never in this line, or in any line this module writes.
    logger.info("mcp: minted a token labelled %s covering %s", name,
                "every bucket" if scope_all else (f"{len(buckets)} bucket(s)" if buckets
                                                  else "no buckets, so nothing"))
    return raw


def revoke_token(store: Store, *, label: str) -> bool:
    """Mark the active token with this label revoked. False when there is no such token.

    The row stays. A credential's job ends when it stops being accepted; the record that the
    deployment once had one is what an operator needs a month later, and it is what makes
    `--list` able to say "this device was revoked" rather than "this device never existed".
    """
    name = _require_text(label, "a token label", limit=LABEL_MAX_CHARS)
    found = _active_by_label(store, name)
    if found is None:
        return False
    key, document = found
    document["revoked_at"] = _store_now()
    store.set_setting(key, json.dumps(document, sort_keys=True))
    logger.info("mcp: revoked the token labelled %s", name)
    return True


def resolve_token(store: Store, raw: str) -> TokenContext | None:
    """The scope this plaintext carries, or None. One digest, one key lookup, no comparison.

    Deliberately not tolerant of anything: the value is used exactly as presented, so a truncated
    bearer string is a different digest and a different key. Revoked is None, malformed is None, and
    an empty scope is a real answer rather than an absent one.
    """
    if not isinstance(raw, str) or not raw or len(raw) > TOKEN_MAX_CHARS:
        return None
    stored = store.get_setting(_token_key(raw))
    if stored is None:
        return None
    try:
        document = _parse(json.loads(stored))
    except ValueError:
        return None
    if document is None or document["revoked_at"] is not None:
        return None
    return TokenContext(label=document["label"], org_ids=frozenset(document["org_ids"]),
                        scope_all=bool(document["scope_all"]))


def _connect(db_path):
    """The one seam the stamp's connection goes through.

    A function rather than a direct `sqlite3.connect`, so a test can count the connections the
    request path opens: "the throttle coalesced" has to be observable, not inferred from a value.
    """
    return sqlite3.connect(db_path)


def _token_use_key(raw: str) -> str:
    return TOKEN_USE_PREFIX + hashlib.sha256(raw.encode()).hexdigest()


def stamp_token_use(db_path: Path | str, raw: str, context: TokenContext | None, *,
                    now: float | None = None) -> bool:
    """Record that a credential that just authenticated was seen. True when a row was written.

    **The one write on the request path, and the narrowest shape it could take.** It is called by
    `StoreTokenVerifier` after a token resolved, and it takes the resolved `TokenContext` as proof:
    without one it returns False before opening anything, so a stranger presenting a guessed bearer
    string costs a digest and no database work at all. It is not a method on `Store`, and the
    `LedgerReader` the handlers hold runs under `PRAGMA query_only=ON`, so nothing reachable from a
    tool call can make this write even if it wanted to.

    Three details, each worth the sentence:

    * its own short-lived connection, opened per write and closed on the way out — the long-lived
      write handle belongs to `serve`, and this process must not acquire a second one that a request
      path can reach;
    * `time.monotonic` for the window, because Docker Desktop's VM clock steps and an NTP correction
      must not freeze the record or release a burst of writes;
    * a failure is a skipped stamp, reported at INFO. A timestamp nobody reads for a week is not
      worth a failed request or a traceback, and the alternative — waiting out a delivery write on a
      client's clock — is the worse trade.
    """
    if context is None or not isinstance(raw, str) or not raw:
        return False
    key = _token_use_key(raw)
    moment = time.monotonic() if now is None else now
    last = _recent_stamps.get(key)
    if last is not None and moment - last < TOKEN_USE_THROTTLE_S:
        return False

    try:
        conn = _connect(db_path)
        try:
            conn.execute(f"PRAGMA busy_timeout={TOKEN_USE_BUSY_TIMEOUT_MS}")
            conn.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?)"
                " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, _store_now()),
            )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error as error:
        logger.info("mcp: could not record that a token was used (%s)", type(error).__name__)
        return False
    _recent_stamps[key] = moment
    return True


def list_tokens(store: Store) -> list[TokenInfo]:
    """Every token row, active and revoked, sorted by label. Never a credential."""
    return [TokenInfo(label=document["label"], org_ids=tuple(document["org_ids"]),
                      scope_all=bool(document["scope_all"]),
                      created_at=document["created_at"], revoked_at=document["revoked_at"],
                      last_used=_last_used(store, key))
            for key, document in sorted(_rows(store), key=lambda pair: pair[1]["label"])]


def _last_used(store: Store, token_key: str) -> str | None:
    """The stamp for one token row, found by digest — the only link that survives a re-mint.

    `stamp_token_use` writes `mcp_token_use:<sha256 of the plaintext>` and the token row is
    `mcp_token:` plus the same digest, so the two are joined by swapping the prefix. Not by label: a
    label is the operator's name for a device and it is deliberately reusable after a revoke, so
    joining on it would let a rotated credential inherit the old device's last use — wrong in the
    dangerous direction, a token nobody has used since the theft reading as alive.

    Nothing derived from the key is returned. The digest identifies the credential as surely as the
    plaintext does, and this column is printed to a terminal somebody may screenshot. The value goes
    through the house sanitizer because a settings row is hand-editable and this table is
    tab-separated: an unsanitized tab forges a column and a newline forges a row.
    """
    raw = store.get_setting(TOKEN_USE_PREFIX + token_key[len(TOKEN_SETTING_PREFIX):])
    if raw is None:
        return None
    return sanitize_event_text(raw, limit=_LAST_USED_MAX_CHARS)


def token_counts(store: Store) -> tuple[int, int]:
    """`(active, revoked)` — the two numbers `check` prints, and all it prints.

    `check` runs in a container whose logs an operator may paste somewhere, and a label names a
    device; the counts answer "is anything listening, and with how many credentials" on their own.
    """
    rows = _rows(store)
    revoked = sum(1 for _, document in rows if document["revoked_at"] is not None)
    return len(rows) - revoked, revoked


# --- the CLI ---------------------------------------------------------------


def add_subparser(subparsers) -> None:
    """Register `mcp-token` on the entry point's subparser table."""
    parser = subparsers.add_parser(
        "mcp-token",
        help="mint, list, or revoke a bearer token for the MCP search server "
             "(the plaintext is printed once and never again)",
    )
    intent = parser.add_mutually_exclusive_group(required=True)
    intent.add_argument("--create", action="store_true",
                        help="mint a token; prints the plaintext exactly once, between two "
                             "warnings, and stores only its sha256")
    intent.add_argument("--list", dest="list", action="store_true",
                        help="label, buckets, created, last used, revoked for every token; "
                             "never a token")
    intent.add_argument("--revoke", action="store_true",
                        help=f"stop one active token accepting requests, by label "
                             f"(its {TOKEN_SETTING_PREFIX.rstrip(':')} row is kept, "
                             "marked revoked)")
    parser.add_argument("--label", default=None,
                        help=f"the device or agent this token belongs to; unique while active, "
                             f"at most {LABEL_MAX_CHARS} characters")
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--org-id", dest="org_id", action="append", default=None,
                       metavar="ID",
                       help=f"a bucket this token may read; repeat it, at most {MAX_ORG_IDS}. "
                            "With none and no --all-buckets the token can authenticate and sees "
                            "nothing")
    scope.add_argument("--all-buckets", dest="all_buckets", action="store_true",
                       help="grant every bucket, including ones that exist later. Also the only "
                            "way to be shown corpus-wide queue depth")


def run_mcp_token(config, args, *, store: Store | None = None) -> int:
    """Do what was asked of the token store. Returns the process exit code.

    `store` is injectable for the same reason `run_ledger` takes its clients: the behaviour that
    matters here is what a credential command does against a store in an awkward state, and that is
    not something to stage by hand.

    `--list` reads and therefore does not migrate, following `events`: a curiosity command must not
    perform a schema change, so an unmigrated store is a stated condition with the fix named.
    `--create` and `--revoke` write, so they migrate first like every other writing command — and a
    mint that cannot write must not print a plaintext that authenticates nothing.
    """
    # A separate name from `store`, so the early refusals below — which happen before any store is
    # opened — cannot leave the `finally` closing something that was never opened. Closing a
    # `None` would replace the sentence the operator came for with an `AttributeError`, which is the
    # shape `run_ledger`'s close-on-the-right-loop fix exists to prevent.
    opened: Store | None = None
    try:
        if store is None:
            db_path = config.data_dir / "state.db"
            if args.list and not db_path.exists():
                # Before Store(): opening the file would create the empty database this command
                # came to report as missing, and the next run would print "no tokens".
                print(f"no database at {db_path}", file=sys.stderr)
                print("run `papra-suite check` first; nothing to list", file=sys.stderr)
                return EXIT_REFUSED
            config.data_dir.mkdir(parents=True, exist_ok=True)
            opened = Store(db_path)
            store = opened
        if args.list:
            if not store.table_columns("settings"):
                print("this store has no settings table yet", file=sys.stderr)
                print("run `papra-suite check` to migrate it; nothing to list", file=sys.stderr)
                return EXIT_REFUSED
            return _print_tokens(store)
        store.migrate()
        if args.create:
            return _create(store, args)
        return _revoke(store, args)
    except TokenError as error:
        print(f"mcp-token refused: {error}", file=sys.stderr)
        return EXIT_REFUSED
    except sqlite3.Error as error:
        print(f"mcp-token refused: the store would not answer ({type(error).__name__}: {error})",
              file=sys.stderr)
        return EXIT_REFUSED
    except KeyboardInterrupt:
        print("mcp-token stopped: interrupted. Nothing was half-written: a mint is one row, and "
              "no plaintext was printed for it", file=sys.stderr)
        return EXIT_INCOMPLETE
    finally:
        if opened is not None:
            opened.close()


def _warn_unknown_buckets(store: Store, grants: dict[str, list[str]], *, stream) -> None:
    """Say so, out loud, when a grant names a bucket this archive has never seen.

    The failure this exists for: `--org-id org-1` when the real bucket is `org-01`. The token
    authenticates, `index_status` answers `scope: buckets`, and every count is zero — which is
    indistinguishable, to whoever holds the token, from a bucket that is genuinely empty. It is the
    shape this project keeps calling a confident wrong answer, and a warning printed once at the
    mint is the cheapest place left to catch it.

    It warns rather than refuses, twice over: a bucket that exists in Papra but is not indexed
    here yet is a legitimate grant, and a mint that fails on a guess would leave the operator unable
    to hand out a credential during the very outage that made one necessary.
    """
    lines = []
    for label, org_ids in grants.items():
        unknown = unknown_buckets(store, org_ids)
        if unknown:
            lines.append(f"{label} grants {', '.join(unknown)}")
    if not lines:
        return
    print(f"warning: {'; '.join(lines)} — no bucket this archive has ever indexed. A typo "
          "authenticates and then reads as an empty archive, which is not something a token holder "
          "can tell apart from a bucket that is genuinely empty. Check the id against Papra's "
          "organisation list; if this bucket is simply new, nothing is wrong and this token works "
          "as soon as it has documents.", file=stream)


def _create(store: Store, args) -> int:
    if not args.label:
        raise TokenError("--create needs --label, the name of the device or agent this token is "
                         "for; without it `--revoke` has nothing to point at")
    _warn_unknown_buckets(store, {args.label: list(args.org_id or [])}, stream=sys.stderr)
    token = mint_token(store, label=args.label, org_ids=args.org_id or [],
                       scope_all=bool(args.all_buckets))
    scope = "every bucket" if args.all_buckets else (
        ", ".join(args.org_id) if args.org_id else "no buckets, so nothing at all")
    print(f"DO NOT LOSE THIS: the token for {args.label!r} ({scope}) is printed once and cannot "
          "be shown again")
    print(token)
    print("That is the only time papra-suite prints it and it cannot be recovered: put it into the "
          "client now, and `papra-suite mcp-token --revoke --label "
          f"{args.label}` if it is lost")
    return EXIT_OK


def _print_tokens(store: Store) -> int:
    tokens = list_tokens(store)
    if not tokens:
        print("no tokens: nothing can call the MCP server's tools until "
              "`papra-suite mcp-token --create --label NAME --org-id ID` mints one")
        return EXIT_OK
    print("label\tbuckets\tcreated\tlast used\trevoked")
    for info in tokens:
        scope = "ALL" if info.scope_all else (",".join(info.org_ids) or "none")
        # `never` is a reading, not a blank: the column exists to tell "not used since it was
        # minted" apart from "nothing is known". Revoking a credential that is in use is the
        # mistake this is meant to prevent.
        print(f"{info.label}\t{scope}\t{info.created_at}\t{info.last_used or 'never'}\t"
              f"{info.revoked_at or 'active'}")
    # A footer, never a column: this table is tab-separated and its `buckets` field is what an
    # operator copies into a client, so a marker in it would read as part of an id.
    _warn_unknown_buckets(store, {info.label: list(info.org_ids) for info in tokens},
                          stream=sys.stdout)
    return EXIT_OK


def _revoke(store: Store, args) -> int:
    if not args.label:
        raise TokenError("--revoke needs --label, the name shown by `papra-suite mcp-token --list`")
    if not revoke_token(store, label=args.label):
        raise TokenError(f"no active token is labelled {args.label!r}; "
                         "run `papra-suite mcp-token --list` to see what is there")
    active, revoked = token_counts(store)
    print(f"revoked the token labelled {args.label!r}: 1 token stops working, {active} still "
          f"active ({revoked} revoked in total)")
    print("A session already open with it stops working on its next request — the scope is read "
          "from the store per request, so there is nothing to restart")
    return EXIT_OK
