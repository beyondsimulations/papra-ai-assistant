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
from dataclasses import dataclass

from papra_suite.logging_setup import SECRET_REGISTRY
from papra_suite.store import Store
from papra_suite.store import _now as _store_now

logger = logging.getLogger("papra_suite.mcp_tokens")

#: Settings-key prefix for one row per token. `Store.list_setting_keys(prefix=...)` enumerates it.
TOKEN_SETTING_PREFIX = "mcp_token:"

#: Bytes of entropy behind `secrets.token_urlsafe`, per the 2026-11-16 spec's Operations section.
TOKEN_ENTROPY_BYTES = 32

#: A bearer header longer than this was not minted here. Hashing an arbitrarily long header is a
#: free-CPU door, so the candidate is bounded before it is digested.
TOKEN_MAX_CHARS = 512

LABEL_MAX_CHARS = 64
ORG_ID_MAX_CHARS = 128

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
    """

    label: str
    org_ids: tuple[str, ...]
    scope_all: bool
    created_at: str
    revoked_at: str | None


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


def _rows(store: Store) -> list[tuple[str, dict]]:
    """Every token row as (key, document), dropping the ones that are not token documents.

    A row under this prefix that does not parse is reported and skipped rather than served: the
    alternative is for a hand-edited or half-written row to become a credential, and for a row whose
    `org_ids` went missing to become a token that grants everything the JSON forgot to restrict.
    """
    found: list[tuple[str, dict]] = []
    for key in store.list_setting_keys(prefix=TOKEN_SETTING_PREFIX):
        raw = store.get_setting(key)
        if raw is None:
            continue
        try:
            document = json.loads(raw)
        except ValueError:
            logger.warning("mcp: settings key %s is not a token row, ignored", key)
            continue
        if _parse(document) is None:
            logger.warning("mcp: settings key %s does not describe a token, ignored", key)
            continue
        found.append((key, document))
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


def list_tokens(store: Store) -> list[TokenInfo]:
    """Every token row, active and revoked, sorted by label. Never a credential."""
    return [TokenInfo(label=document["label"], org_ids=tuple(document["org_ids"]),
                      scope_all=bool(document["scope_all"]),
                      created_at=document["created_at"], revoked_at=document["revoked_at"])
            for _, document in sorted(_rows(store), key=lambda pair: pair[1]["label"])]


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
                        help="print label, buckets, created and revoked for every token; "
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


def _create(store: Store, args) -> int:
    if not args.label:
        raise TokenError("--create needs --label, the name of the device or agent this token is "
                         "for; without it `--revoke` has nothing to point at")
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
    print("label\tbuckets\tcreated\trevoked")
    for info in tokens:
        scope = "ALL" if info.scope_all else (",".join(info.org_ids) or "none")
        print(f"{info.label}\t{scope}\t{info.created_at}\t{info.revoked_at or 'active'}")
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
