"""The sender allow-list: who may put anything into Papra at all.

SCOPE, stated narrowly on purpose. This is a permission check over the visible
From: address — a header the sender writes. It stops strangers, harvested
addresses and bulk mail. It does NOT establish that the named person sent the
message: that needs the provider's own authentication verdict, and until we can
show a provider strips a sender-supplied authentication header, no part of this
module pretends to detect spoofing.

Fail-closed rules baked in here:

* An empty list denies everything. A configuration typo should produce a quiet
  inbox the operator notices immediately, not an open door nobody notices.
* A nonsense entry is a ConfigError at parse time, never a silently dropped
  entry — an operator reading their own config must not be lied to.
* A malformed From: is a distinct verdict from "unknown sender". Reporting a
  forged header as "a sender you might want to allow" teaches the operator to
  add the wrong thing.
* No plus-address stripping. Plus-addressing means different things on
  different providers, so stripping it would silently widen admission.
* Two From: addresses are malformed rather than a free choice: whichever one
  the code picked, the attacker chose the identity that got checked.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from email.utils import getaddresses, parseaddr

from papra_suite.config import ConfigError

# Sentinel for a message whose From: cannot be a single identity: no From at
# all, or more than one From field. It never matches the address grammar, so
# the parser reports it as ambiguous on its own — joining duplicate fields
# instead would let "a@x.com" plus an empty field collapse into one
# apparently valid address.
AMBIGUOUS_FROM = "\u0000ambiguous-from"

PERMISSION = "allow"
DENY_SENDER = "deny_sender"
DENY_MALFORMED = "deny_malformed"

_ADDRESS_ENTRY = re.compile(r"^[^@\s]+@[^@\s]+$")
_DOMAIN_ENTRY = re.compile(
    r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$"
)


# An identity is an address, not prose. Decoding an RFC 2047 word BEFORE
# deciding what the header says would let encoded content become structural
# punctuation or address text, so the header is parsed as it arrived and an
# encoded word anywhere in the address simply fails the strict shape below.
# Display names are never decoded: they carry no authorization weight.
_ENCODED_WORD = re.compile(r"=\?", re.IGNORECASE)
_STRICT_ADDRESS = re.compile(
    r"^[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)+$"
)


def parse_from_header(value: str | None) -> str | None:
    """The single visible sender address, lowercased, or None if ambiguous.

    "Ambiguous" covers a missing header, a header with no address in it, more
    than one address, an encoded word, and anything whose shape is not a plain
    address. Ambiguity is refused rather than interpreted: whichever address
    the code picked out of an ambiguous header, the sender chose it.
    """
    if not value:
        return None
    pairs = getaddresses([value])
    if len(pairs) != 1:
        return None
    # getaddresses is a parser, not a validator: it happily returns "a@x.com"
    # for "a@x.com garbage", discarding whatever the sender wrote after the
    # address. So the whole value must account for itself — either an angleaddr
    # that ends the value, or nothing but the address. Junk after the address
    # means the header is not saying one thing, and we refuse.
    # Check the header's OWN text, because the parsed output cannot be trusted
    # to resemble it: getaddresses("a@x.com more") returns "a@x.commore" — the
    # space is deleted and the words glued into an address nobody wrote. A rule
    # over the parsed value would pass that through as a valid domain.
    text = value.strip()
    addr = parseaddr(text)[1].strip().lower()
    if "<" in text:
        # An angle-addr must end the value; anything after it is a second claim.
        if not text.endswith(">"):
            return None
    else:
        # Without angle brackets the value must be the address and nothing else.
        # Legacy "Display Name a@x.com" is refused with it: ambiguous, and not
        # what any current mail client emits.
        if any(c.isspace() for c in text) or text.lower() != addr:
            return None
    if _ENCODED_WORD.search(addr) or not _STRICT_ADDRESS.match(addr):
        return None
    return addr


def extract_from_field(header: bytes) -> str:
    """The single From: value out of raw header bytes, or AMBIGUOUS_FROM.

    Written against bytes rather than a parsed message because admission happens
    on what the server sent, before any body is requested, and because a MIME
    parser that helpfully resolves duplicates is doing exactly the thing this
    function exists to refuse. Folded continuation lines are joined; a repeated
    field is ambiguous; undecodable bytes are refused rather than raised — the
    header is attacker-supplied input.
    """
    text = header.decode("ascii", errors="replace") if isinstance(header, bytes) else header
    fields: list[tuple[str, str]] = []
    name = value = None

    def flush() -> None:
        nonlocal name, value
        if name is not None:
            fields.append((name, value))
        name = value = None

    for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        if not line:
            break  # end of headers; a "From:" line in the body is not a header
        if line[0] in " \t" and name is not None:
            value = f"{value} {line.strip()}"
            continue
        flush()
        head, sep, rest = line.partition(":")
        if sep:
            name, value = head.strip().lower(), rest.strip()
    flush()

    values = [v for n, v in fields if n == "from"]
    return values[0] if len(values) == 1 else AMBIGUOUS_FROM


@dataclass(frozen=True)
class AllowList:
    addresses: frozenset[str]
    domains: frozenset[str]
    configured: bool

    def permits_sender(self, address: str | None) -> str:
        """PERMISSION or DENY_SENDER for an already-parsed address."""
        if not address:
            return DENY_SENDER
        if address in self.addresses:
            return PERMISSION
        domain = address.rsplit("@", 1)[1]
        if domain in self.domains:
            return PERMISSION
        return DENY_SENDER

    def verdict_for_header(self, value: str | None) -> str:
        """Verdict straight from a raw From: header value."""
        address = parse_from_header(value)
        if address is None:
            return DENY_MALFORMED
        return self.permits_sender(address)

    @property
    def paused(self) -> bool:
        """No usable entries: ingest must not fetch, ingest or retire anything."""
        return not self.configured

    def admit_headers(self, header: bytes) -> str:
        """Verdict straight from raw header bytes — the fetch-time gate."""
        return self.verdict_for_header(extract_from_field(header))

    def describe(self) -> str:
        if not self.configured:
            return ("allow-list is EMPTY: this deployment denies every sender "
                    "(set PAPRA_ALLOWED_SENDERS to allow any mail in)")
        parts = sorted(self.addresses | {f"@{d}" for d in self.domains})
        return ", ".join(parts)


def parse_allowlist(value: str | None) -> AllowList:
    """Parse PAPRA_ALLOWED_SENDERS: comma-separated addresses and domains."""
    addresses: set[str] = set()
    domains: set[str] = set()
    for raw in (value or "").split(","):
        entry = raw.strip().lower()
        if not entry:
            continue
        if entry.startswith("@"):
            entry = entry[1:]
        if _STRICT_ADDRESS.match(entry):
            addresses.add(entry)
            continue
        if _DOMAIN_ENTRY.match(entry):
            domains.add(entry)
            continue
        raise ConfigError(
            "PAPRA_ALLOWED_SENDERS contains an entry that is neither an email "
            f"address nor a domain: {entry!r}. Expected e.g. "
            "someone@example.com or example.com"
        )
    return AllowList(
        addresses=frozenset(addresses),
        domains=frozenset(domains),
        configured=bool(addresses or domains),
    )
