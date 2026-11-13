"""Mail ingestion: parsing helpers, the sync IMAP session, and the poller.

This module owns the only imaplib usage in the service; every IMAP call is
executed via asyncio.to_thread by the poller (decisions doc §3).
"""

import asyncio
import hashlib
import imaplib
import logging
import re
import sqlite3
import ssl
import time
from dataclasses import dataclass, field
from email import message_from_bytes
from email.header import decode_header, make_header
from email.message import Message

from papra_suite.logging_setup import SECRET_REGISTRY
from papra_suite.papra_client import PapraClient, PapraError
from papra_suite.pipeline import enqueue_ocr_job
from papra_suite.safematch import KillableMatcher, MatchOutcome
from papra_suite.store import Mailbox, Org, Store

logger = logging.getLogger(__name__)

# imaplib's implicit TLS context verifies NOTHING (CERT_NONE, no hostname
# check): an on-path attacker with any self-signed certificate would capture
# the mailbox LOGIN credentials. One shared module-level context carries the
# platform trust store and hostname checking into every TLS connection
# (contexts are safe to share: the handshake is per-connection state).
_SSL_CONTEXT = ssl.create_default_context()

MAX_FILENAME_LENGTH = 120
PDF_MAGIC = b"%PDF-"

# Memory hardening for fetch_since: a single fetched message larger than
# MAX_MESSAGE_BYTES is skipped (counted as processed so the poll advances);
# a poll stops fetching once accumulated bytes exceed MAX_POLL_BYTES — the
# remaining messages stay for the next poll (correct re-fetch semantics).
MAX_MESSAGE_BYTES = 25 * 1024 * 1024
MAX_POLL_BYTES = 100 * 1024 * 1024
# Subject cap for the regex filter: RFC 5322 sets no hard subject limit, and
# legitimately useful subjects beyond this length are vanishingly rare, so
# the cap shrinks what a hostile subject can hand the engine. It is NOT the
# bound that protects the process: cutting a subject from 5000 to 1000 chars
# still leaves a catastrophic pattern running for longer than any timeout,
# which is why the match itself is killable (SUBJECT_FILTER_TIMEOUT_S).
MAX_SUBJECT_CHARS = 1000
# Per-message budget for the operator's subject regex. The match runs in the
# disposable child of papra_suite/regex_worker.py, and a child can be killed:
# SIGTERM/SIGKILL are delivered by the kernel, so this budget interrupts a
# catastrophic backtracking search mid-flight. A wait_for around a search in
# THIS process could not (re.search holds the GIL until it drains, so the
# callback only fires after the freeze is over). A killed match counts as a
# NON-MATCH and the poll continues, so \Seen and the UID cursor still advance
# and a poisoned subject cannot re-freeze the process on the next poll.
SUBJECT_FILTER_TIMEOUT_S = 2.0
# Budget for the pre-validation probes, which run in the same killable child.
# Validation used to be the unbounded part: (a+)+\d+$ never returns from its
# own "a"*30 probe, which froze the loop before a single mail was read. 10s
# is generous — a pattern that cannot finish a 30-byte probe in that time is
# definitionally unusable against a 1000-byte subject, so it is rejected.
SUBJECT_VALIDATION_TIMEOUT_S = 10.0
# Pre-validation growth ratio: t(long near-miss) / t(short near-miss) above
# this rejects the pattern. HONEST SCOPE — a probe can only measure the
# backtracking its INPUT reaches, so the gate catches patterns whose ambiguous
# group is reachable from the probe alphabet ((a|aa)+b ~120x, (a+)+$ ~15x,
# ^(\w+\s?)*$ ~15x) and MISSES patterns whose blowup sits behind an anchor
# plus a literal the operator typed: (Rechnung|Invoice)\s*(\d+\s*)+$ scores
# ~1x on every probe here and is ACCEPTED while still costing ~2x per added
# digit. The bound that protects the process is killable execution, not this
# gate; passing it must never be read as "this pattern is safe".
SUBJECT_FILTER_MAX_GROWTH_RATIO = 10
# Probe pairs for the gate, each a NEAR-MISS at two lengths (a trailing byte
# that breaks the match, so backtracking actually runs — an input the pattern
# MATCHES is O(n) and measures nothing). The original all-"a" pair stays:
# it is what catches (a|aa)+b. The two near-miss pairs add the classes that
# need a non-matching tail to blow up ((a+)+$, ^(\w+\s?)*$) at lengths small
# enough to stay milliseconds — the gate is a cheap first filter now that the
# killable child is the actual bound.
SUBJECT_FILTER_PROBE_PAIRS = (
    ("a" * 20, "a" * 30),
    ("a" * 12 + "!", "a" * 16 + "!"),
    ("1" * 12 + "!", "1" * 16 + "!"),
)


@dataclass
class Attachment:
    filename: str
    content: bytes


@dataclass
class FetchBatch:
    """Result of one fetch_since pass.

    messages: (uid, raw) pairs below the per-message cap, in uid order.
    oversized_uids: messages above MAX_MESSAGE_BYTES — the poller skips
    them without parsing but still advances last_seen past them.
    """
    messages: list[tuple[int, bytes]] = field(default_factory=list)
    oversized_uids: list[int] = field(default_factory=list)


@dataclass
class ParsedMessage:
    message_id: str | None
    subject: str | None
    pdf_attachments: list[Attachment] = field(default_factory=list)
    skipped_attachments: list[str] = field(default_factory=list)


def sniff_is_pdf(data: bytes) -> bool:
    """PDF detection by magic bytes only — mail clients mislabel mime types."""
    return data.startswith(PDF_MAGIC)


def sanitize_filename(name: str | None) -> str:
    """Strip path components and control chars; cap length; never empty."""
    if not name:
        return "document.pdf"
    # take the basename regardless of separator style
    name = re.split(r"[/\\]", name)[-1]
    name = "".join(ch for ch in name if ch.isprintable() and not ch.isspace() or ch == " ")
    name = name.strip()
    stem, dot, _suffix = name.rpartition(".")
    if not name or (dot and not stem.strip()):
        return "document.pdf"
    name = name.strip(".")
    if not name:
        return "document.pdf"
    if len(name) > MAX_FILENAME_LENGTH:
        stem, dot, suffix = name.rpartition(".")
        if dot and len(suffix) <= 10:
            name = name[: MAX_FILENAME_LENGTH - len(suffix) - 1] + "." + suffix
        else:
            name = name[:MAX_FILENAME_LENGTH]
    return name


def _attachment_name(part: Message) -> str:
    raw = part.get_filename() or "attachment"
    return sanitize_filename(raw)


def parse_message(raw: bytes) -> ParsedMessage:
    """Parse a raw MIME message; classify attachments by magic bytes, not labels."""
    message = message_from_bytes(raw)
    parsed = ParsedMessage(
        message_id=message.get("Message-ID"),
        subject=_decode_header_value(message.get("Subject")),
    )
    for part in message.walk():
        if part.get_content_maintype() == "multipart":
            continue
        payload = part.get_payload(decode=True)
        filename = part.get_filename()
        if payload is None:
            # message/rfc822 forwards carry a nested message, not bytes (the
            # walk below already descends into them and extracts their PDFs).
            # A NAMED part landing here is an attachment the spec says must
            # be logged and skipped — never silently dropped into neither list.
            if filename is not None:
                name = sanitize_filename(_decode_header_value(filename))
                parsed.skipped_attachments.append(name)
                logger.info("skipped non-pdf attachment %s (no decodable payload)", name)
            continue
        if filename is None:
            # body text or an inline part without a name: not an attachment —
            # unless the payload smells like a PDF (body text never starts
            # with %PDF-): a filename-less application/pdf part must be
            # ingested under the fallback name, not silently dropped.
            if sniff_is_pdf(payload):
                parsed.pdf_attachments.append(Attachment(sanitize_filename(None), payload))
            continue
        # RFC 2047 encoded words survive get_filename() undecoded; decode
        # before sanitizing or the encoded word becomes the document name
        # (and M3's title worker falls back to it). Same ruling as the subject.
        filename = _decode_header_value(filename)
        if sniff_is_pdf(payload):
            parsed.pdf_attachments.append(Attachment(sanitize_filename(filename), payload))
        else:
            parsed.skipped_attachments.append(sanitize_filename(filename))
    return parsed


def _decode_header_value(raw: str | None) -> str | None:
    """Decode RFC 2047 encoded words (e.g. =?utf-8?Q?...?=) so the subject
    filter matches German mail; fall back to the raw string on any error."""
    if raw is None:
        return None
    try:
        return str(make_header(decode_header(raw)))
    except Exception:  # noqa: BLE001 - decoding must never break parsing
        return raw


def validate_subject_filter(pattern: str | None) -> None:
    r"""Pre-validate a mailbox subject filter once per poll, before any
    message processing. Raises ValueError on an invalid or unsafe pattern.

    WHAT THIS GATE ACTUALLY DETECTS: it times the compiled pattern against a
    few short probe pairs (SUBJECT_FILTER_PROBE_PAIRS) and refuses when the
    growth ratio passes SUBJECT_FILTER_MAX_GROWTH_RATIO. A probe can only
    measure the backtracking its INPUT reaches, so it catches patterns whose
    ambiguous group is reachable from the probe alphabet ((a|aa)+b, (a+)+$,
    ^(\w+\s?)*$) and does NOT catch patterns whose ambiguity sits behind an
    anchor plus a literal the operator typed:
    (Rechnung|Invoice)\s*(\d+\s*)+$ scores ~1x on every probe here and is
    accepted, while still costing ~2x per added digit on "Rechnung 111...!".
    Passing this function does NOT mean a pattern is safe.

    What protects the process is that the filter is EXECUTED KILLABLY: the
    poller runs both this gate and every per-message match in the child
    process of papra_suite/regex_worker.py (see papra_suite.safematch), so a
    probe that never terminates — (a+)+\d+$ never returns from its own
    "a"*30 probe — is killed at SUBJECT_VALIDATION_TIMEOUT_S and surfaces as
    this same ValueError, and a subject that blows up at match time is killed
    at SUBJECT_FILTER_TIMEOUT_S and counted as a non-match. The function
    itself stays sync and cheap so it can be called directly; only there is
    its runtime bounded by nothing.
    """
    if not pattern:
        return
    try:
        compiled = re.compile(pattern)
    except re.error as error:
        raise ValueError(f"invalid subject filter: {error}") from error
    for short_input, long_input in SUBJECT_FILTER_PROBE_PAIRS:
        start = time.perf_counter()
        compiled.search(short_input)
        t_short = time.perf_counter() - start
        start = time.perf_counter()
        compiled.search(long_input)
        t_long = time.perf_counter() - start
        if t_long / max(t_short, 1e-6) > SUBJECT_FILTER_MAX_GROWTH_RATIO:
            raise ValueError(
                "subject filter is unsafe: super-linear backtracking "
                f"(probe growth ratio over {SUBJECT_FILTER_MAX_GROWTH_RATIO})"
            )


def subject_matches(subject: str | None, pattern: str | None) -> bool:
    """Optional per-mailbox subject regex filter; no pattern means match all.

    The subject is truncated to MAX_SUBJECT_CHARS before matching: RFC 5322
    has no hard subject limit, legit subjects over the cap are vanishingly
    rare, and the cap shrinks what the engine has to chew on.

    This is the SYNC reference for filter semantics (case-insensitivity, the
    empty-pattern and None-subject branches, the cap) — and running the regex
    in the calling thread is exactly why it is no longer the production path:
    a catastrophic pattern here is uninterruptible, because re.search holds
    the GIL until it drains and no timeout inside this process can fire.
    The poller asks papra_suite.safematch.KillableMatcher the same question
    instead: same answer, but of a child process that is killed when it runs
    out of budget.
    """
    if not pattern:
        return True
    if subject is None:
        return False
    if len(subject) > MAX_SUBJECT_CHARS:
        subject = subject[:MAX_SUBJECT_CHARS]
    return re.search(pattern, subject, re.IGNORECASE) is not None


def dedup_hash(mailbox_id: str, message_id: str | None, filename: str, size: int) -> str:
    basis = f"{mailbox_id}\x00{message_id or ''}\x00{filename}\x00{size}"
    return hashlib.sha256(basis.encode()).hexdigest()


def compute_file_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class MailboxSession:
    """Sync imaplib wrapper — every method runs inside asyncio.to_thread."""

    def __init__(self, *, host: str, port: int, username: str, password: str,
                 ssl_mode: str) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._ssl_mode = ssl_mode
        self._imap: imaplib.IMAP4 | None = None

    def connect(self) -> None:
        # Fail-closed on an unrecognized ssl_mode (legacy/hand-edited rows):
        # an unknown value must never silently fall through to plaintext
        # IMAP, which would send the credentials in cleartext.
        # timeout: a stalled server must fail the poll (and count toward the
        # failure accounting), not wedge the poll task forever on a socket read
        if self._ssl_mode == "ssl":
            self._imap = imaplib.IMAP4_SSL(
                self._host, self._port, ssl_context=_SSL_CONTEXT, timeout=30
            )
        elif self._ssl_mode == "starttls":
            self._imap = imaplib.IMAP4(self._host, self._port, timeout=30)
            self._imap.starttls(ssl_context=_SSL_CONTEXT)
        elif self._ssl_mode == "plain":
            self._imap = imaplib.IMAP4(self._host, self._port, timeout=30)
        else:
            raise ValueError(
                f"unknown ssl_mode {self._ssl_mode!r}; refusing to connect "
                "(valid: ssl, starttls, plain)"
            )
        self._imap.login(self._username, self._password)

    def select_inbox(self) -> str:
        status, _ = self._imap.select("INBOX")
        if status != "OK":
            raise imaplib.IMAP4.error("select INBOX failed")
        _, data = self._imap.response("UIDVALIDITY")
        # data is [b'UIDVALIDITY <n>'] after select; be defensive: some servers
        # (and imaplib versions) return None/empty — fall back to "0" rather
        # than crash (Task 5's integration tests exercise the real path).
        if not data or not data[0]:
            return "0"
        return data[0].decode().split()[-1]

    def fetch_since(self, last_seen_uid: int | None) -> FetchBatch:
        if last_seen_uid is None:
            status, data = self._imap.uid("SEARCH", None, "ALL")
        else:
            status, data = self._imap.uid("SEARCH", None, f"UID {last_seen_uid + 1}:*")
        if status != "OK":
            raise imaplib.IMAP4.error("UID SEARCH failed")
        uids = [int(u) for u in data[0].split()]
        # RFC 3501 edge: "UID n:*" must include the mailbox's last message even
        # when its uid is below n — without this filter such servers would cause
        # a perpetual re-fetch of the last message (benign but wasteful).
        uids = [u for u in uids if last_seen_uid is None or u > last_seen_uid]
        uids.sort()
        batch = FetchBatch()
        total_bytes = 0
        for uid in uids:
            if total_bytes > MAX_POLL_BYTES:
                remaining = len(uids) - len(batch.messages) - len(batch.oversized_uids)
                logger.info(
                    "poll byte cap reached (%d > %d); %d message(s) left for the next poll",
                    total_bytes, MAX_POLL_BYTES, remaining,
                )
                break
            # Pre-check the message size via a cheap metadata-only fetch
            # (RFC822.SIZE) before requesting the body: a hostile oversized
            # message must not be fully read into RAM just to be skipped.
            size = self._rfc822_size(uid)
            if size is None:
                logger.warning("message uid=%s vanished before fetch; skipping", uid)
                continue
            total_bytes += size
            if size > MAX_MESSAGE_BYTES:
                # Oversized messages still count toward the aggregate cap,
                # exactly as when the bytes crossed the wire into this process.
                logger.warning(
                    "message uid=%s exceeds the per-message cap (%d bytes); "
                    "skipping without fetching the body",
                    uid, MAX_MESSAGE_BYTES,
                )
                batch.oversized_uids.append(uid)
                continue
            status, fetch_data = self._imap.uid("FETCH", str(uid), "(BODY.PEEK[])")
            if status != "OK":
                raise imaplib.IMAP4.error(f"UID FETCH failed for {uid}")
            # A message can vanish between SEARCH and FETCH (another client
            # expunged it): the response is then a bare bytes status line or
            # [None]. Validate the shape instead of indexing blindly — an
            # int/TypeError here would fail the whole poll.
            entry = fetch_data[0] if fetch_data else None
            if not (isinstance(entry, tuple) and len(entry) >= 2
                    and isinstance(entry[1], bytes)):
                logger.warning("message uid=%s vanished before fetch; skipping", uid)
                continue
            batch.messages.append((uid, entry[1]))
        return batch

    def _rfc822_size(self, uid: int) -> int | None:
        """Return the message's RFC822.SIZE, or None if it vanished/parse failed."""
        status, fetch_data = self._imap.uid("FETCH", str(uid), "(RFC822.SIZE)")
        if status != "OK":
            raise imaplib.IMAP4.error(f"UID FETCH failed for {uid}")
        entry = fetch_data[0] if fetch_data else None
        if not isinstance(entry, bytes):
            return None
        match = re.search(rb"RFC822\.SIZE\s+(\d+)", entry)
        return int(match.group(1)) if match else None

    def mark_seen(self, uids: list[int]) -> None:
        for uid in uids:
            status, _data = self._imap.uid("STORE", str(uid), "+FLAGS", "(\\Seen)")
            if status != "OK":
                # a NO here means the flags were NOT set: the message would be
                # re-fetched forever without surfacing — raise into the
                # poller's never-raises guard (failure accounting engages)
                raise imaplib.IMAP4.error(f"UID STORE \\Seen failed for {uid}")

    def delete_and_expunge(self, uids: list[int]) -> None:
        if not uids:
            return
        for uid in uids:
            status, _data = self._imap.uid("STORE", str(uid), "+FLAGS", "(\\Deleted)")
            if status != "OK":
                raise imaplib.IMAP4.error(f"UID STORE \\Deleted failed for {uid}")
        # imaplib parses the CAPABILITY response at connect time and exposes
        # it as `capabilities` (an uppercase tuple of extension names).
        capabilities = getattr(self._imap, "capabilities", ()) or ()
        if "UIDPLUS" in capabilities:
            # RFC 4315 UID EXPUNGE: only our uids are removed — an operator's
            # flagged-but-unexpunged message is untouched.
            status, _data = self._imap.uid(
                "EXPUNGE", ",".join(str(uid) for uid in uids)
            )
            if status != "OK":
                raise imaplib.IMAP4.error("UID EXPUNGE failed")
            return
        # Fallback (no UIDPLUS): a plain EXPUNGE destroys EVERY \Deleted
        # message in the mailbox — including mail an operator flagged in
        # their own client. Only expunge when the deleted set is exactly ours;
        # otherwise skip: our \Deleted flags stay (harmless — the operator's
        # client expunges their own mail), and the messages are re-fetched
        # next poll but deduped.
        status, data = self._imap.uid("SEARCH", None, "DELETED")
        if status != "OK":
            raise imaplib.IMAP4.error("UID SEARCH DELETED failed")
        deleted = {int(u) for u in data[0].split()} if data and data[0] else set()
        if deleted == set(uids):
            status, _data = self._imap.expunge()
            if status != "OK":
                raise imaplib.IMAP4.error("EXPUNGE failed")
        else:
            logger.warning(
                "skipping expunge: %d foreign \\Deleted message(s) in the "
                "mailbox; the operator's client will expunge their own mail",
                len(deleted - set(uids)),
            )

    def close(self) -> None:
        if self._imap is not None:
            try:
                self._imap.logout()
            except Exception:  # noqa: BLE001 - closing must never raise
                pass
            self._imap = None


@dataclass
class PollResult:
    messages_scanned: int = 0
    documents_uploaded: int = 0
    attachments_skipped: int = 0
    messages_failed: int = 0
    error: str | None = None
    # appended last: the pre-existing fields keep their positional order
    oversized_skipped: int = 0


async def poll_mailbox_once(store: Store, client: PapraClient,
                            mailbox: Mailbox, org: Org, *,
                            matcher: KillableMatcher | None = None) -> PollResult:
    """Poll one mailbox once. Never raises: failures land in the result and
    the mailbox's persisted error state (3 consecutive -> disabled).

    `matcher` is the killable regex child that runs the subject filter (see
    papra_suite.safematch). The LIFECYCLE SEAM is the poll task:
    papra_suite.pipeline._poll_loop builds one matcher for its whole life and
    hands it to every poll, so the child is reused across polls and across
    all messages of a poll and is killed together with the task (serve
    shutdown, mailbox deleted/disabled, task death). A caller that injects
    nothing gets a private matcher, closed on the way out of here — so the
    killable filter cannot be forgotten by a caller, and no path leaves an
    orphan regex_worker behind.
    """
    if matcher is not None:
        return await _poll_mailbox_once(store, client, mailbox, org, matcher)
    own_matcher = KillableMatcher()
    try:
        return await _poll_mailbox_once(store, client, mailbox, org, own_matcher)
    finally:
        await own_matcher.aclose()


async def _subject_filter_match(matcher: KillableMatcher, subject: str | None,
                                pattern: str | None, *, uid: int) -> bool:
    r"""Ask the killable child whether this subject passes the filter.

    Mirrors subject_matches' cheap branches (no pattern matches everything,
    a missing subject matches nothing) so the sync helper and the production
    path keep answering the same question, then runs the actual regex in the
    child. A killed match (TIMEOUT) and a child that cannot run at all
    (UNAVAILABLE) are both NON-MATCHES plus a WARNING: the poll continues,
    \Seen and the cursor advance, and the next poll is not poisoned again.
    Degrading to a non-match rather than to a match is deliberate — an
    unanswered filter must never ingest mail on its own.
    """
    if not pattern:
        return True
    if subject is None:
        return False
    outcome = await matcher.search(pattern, subject[:MAX_SUBJECT_CHARS],
                                   timeout_s=SUBJECT_FILTER_TIMEOUT_S)
    if outcome is MatchOutcome.TIMEOUT:
        logger.warning(
            "subject filter timed out after %.1fs on uid=%s and the match was "
            "killed; treating the message as a non-match",
            SUBJECT_FILTER_TIMEOUT_S, uid,
        )
    elif outcome is MatchOutcome.UNAVAILABLE:
        logger.warning(
            "subject filter could not be evaluated on uid=%s (no regex child "
            "available); treating the message as a non-match", uid
        )
    return outcome is MatchOutcome.MATCH


async def _poll_mailbox_once(store: Store, client: PapraClient, mailbox: Mailbox,
                             org: Org, matcher: KillableMatcher) -> PollResult:
    SECRET_REGISTRY.add(mailbox.password)
    session = MailboxSession(
        host=mailbox.host, port=mailbox.port, username=mailbox.username,
        password=mailbox.password, ssl_mode=mailbox.ssl_mode,
    )
    result = PollResult()
    try:
        # Reject unsafe or uncompilable subject filters BEFORE connecting and
        # before ANY message processing: a ValueError here flows into the
        # poll-failure path (3-strike disable, visible in the dashboard)
        # instead of the process paying for the pattern one message at a time.
        # It runs in the killable child, so a pattern whose probe never
        # terminates ((a+)+\d+$) is killed and rejected here rather than
        # freezing the loop before a single mail is read.
        await matcher.validate(mailbox.subject_filter,
                               timeout_s=SUBJECT_VALIDATION_TIMEOUT_S)
        await asyncio.to_thread(session.connect)
        uidvalidity = await asyncio.to_thread(session.select_inbox)
        rescan = mailbox.uidvalidity is not None and mailbox.uidvalidity != uidvalidity
        last_seen = None if (mailbox.last_seen_uid is None or rescan) else mailbox.last_seen_uid
        batch = await asyncio.to_thread(session.fetch_since, last_seen)
    except Exception as error:  # noqa: BLE001 - the poller never crashes the loop
        result.error = str(error)
        await asyncio.to_thread(session.close)
        _record_failure_quietly(store, mailbox.id, result.error)
        return result

    seen_uids: list[int] = []
    delete_uids: list[int] = []
    candidate_last_seen = mailbox.last_seen_uid if not rescan else None
    prefix_failed = False

    try:
        # merge messages and oversized uids back into uid order so the
        # freeze-on-failure semantics stay position-correct
        entries = sorted(
            [(uid, raw) for uid, raw in batch.messages]
            + [(uid, None) for uid in batch.oversized_uids]
        )
        for uid, raw in entries:
            result.messages_scanned += 1
            if raw is None:
                logger.warning(
                    "message uid=%s skipped: exceeds the per-message size cap", uid
                )
                # counted per processed message, exactly like messages_scanned:
                # a mid-batch exception leaves the later uids uncounted
                result.oversized_skipped += 1
                seen_uids.append(uid)
                if not prefix_failed:
                    candidate_last_seen = uid
                continue
            # parse_message on a crafted 22MB multipart measured a 238ms
            # event-loop block: MIME parsing must run off the loop like every
            # other blocking call in this poller (decisions doc §3)
            parsed = await asyncio.to_thread(parse_message, raw)
            # The admin-supplied regex runs in the killable child, off this
            # process entirely. A pattern that passes pre-validation but
            # blows up behind its own literal ((Rechnung|Invoice)\s*(\d+\s*)+$
            # on "Rechnung 111...!") is killed at SUBJECT_FILTER_TIMEOUT_S:
            # the kernel delivers the signal, so unlike a wait_for around a
            # search here (which cannot fire while re.search holds the GIL)
            # the budget actually ends the match, the event loop stays free
            # for the whole run, and the cursor still advances below.
            matched = await _subject_filter_match(
                matcher, parsed.subject, mailbox.subject_filter, uid=uid
            )
            if not matched:
                logger.info("message uid=%s skipped by subject filter", uid)
                seen_uids.append(uid)
                if not prefix_failed:
                    candidate_last_seen = uid
                continue
            uploads_ok = True
            had_pdfs = False
            for attachment in parsed.pdf_attachments:
                had_pdfs = True
                attachment_hash = dedup_hash(
                    mailbox.id, parsed.message_id, attachment.filename, len(attachment.content)
                )
                if store.dedup_exists(attachment_hash):
                    logger.info("attachment %s already ingested (dedup)", attachment.filename)
                    continue
                try:
                    document = await client.create_document(
                        org.papra_org_id, attachment.filename, attachment.content
                    )
                except PapraError as error:
                    if error.status == 409:
                        # Papra dedups by content hash per org: a re-sent
                        # document 409s with document.already_exists. That is
                        # an already-ingested SUCCESS, not a failure — record
                        # the dedup hash (no re-upload on later polls), no OCR
                        # job (the M3 sweep covers pre-existing documents).
                        store.add_dedup(attachment_hash)
                        logger.info(
                            "attachment %s already ingested in papra (409)",
                            attachment.filename,
                        )
                        continue
                    uploads_ok = False
                    result.messages_failed += 1
                    logger.error("upload failed for %s: %s", attachment.filename, error)
                    continue
                file_hash = compute_file_hash(attachment.content)
                enqueue_ocr_job(
                    store, papra_org_id=org.papra_org_id, document_id=document["id"],
                    file_hash=file_hash, name=document["name"],
                    mime="application/pdf", ingested_via="mail",
                )
                store.add_dedup(attachment_hash)
                result.documents_uploaded += 1
            for name in parsed.skipped_attachments:
                result.attachments_skipped += 1
                logger.info("skipped non-pdf attachment %s", name)
            if uploads_ok:
                seen_uids.append(uid)
                if mailbox.delete_after_download and had_pdfs:
                    delete_uids.append(uid)
                if not prefix_failed:
                    candidate_last_seen = uid
            else:
                # freeze last_seen: a failed message must stay re-fetchable;
                # later messages are still processed best-effort (dedup-safe)
                prefix_failed = True
    except Exception as error:  # noqa: BLE001 - the poller never raises
        result.error = str(error)
        await asyncio.to_thread(session.close)
        _record_failure_quietly(store, mailbox.id, result.error)
        return result

    try:
        await asyncio.to_thread(session.mark_seen, seen_uids)
        if delete_uids:
            await asyncio.to_thread(session.delete_and_expunge, delete_uids)
    except Exception as error:  # noqa: BLE001
        result.error = str(error)
        await asyncio.to_thread(session.close)
        _record_failure_quietly(store, mailbox.id, result.error)
        return result
    finally:
        await asyncio.to_thread(session.close)

    # record success even for an empty poll: last_poll_at must refresh and
    # a previous error must clear; uid 0 means "nothing seen yet"
    # the fallback is always 0: this arm runs only when candidate_last_seen is
    # None, i.e. on a rescan or when last_seen_uid was None — nothing to carry forward
    last_seen = candidate_last_seen if candidate_last_seen is not None else 0
    _record_success_quietly(store, mailbox.id, last_seen_uid=last_seen, uidvalidity=uidvalidity)
    return result


def _record_success_quietly(store: Store, mailbox_id: str, *, last_seen_uid: int,
                            uidvalidity: str | None) -> None:
    try:
        store.record_poll_success(mailbox_id, last_seen_uid=last_seen_uid, uidvalidity=uidvalidity)
    except (ValueError, sqlite3.Error) as error:
        # mailbox not persisted (unit fixtures) or the store is unavailable
        # (locked db, disk full): the poller's never-raises guarantee holds
        logger.error("failed to record poll success for mailbox %s: %s", mailbox_id, error)


def _record_failure_quietly(store: Store, mailbox_id: str, error: str) -> None:
    try:
        store.record_poll_failure(mailbox_id, error)
    except (ValueError, sqlite3.Error) as record_error:
        # mailbox not persisted (unit fixtures) or the store is unavailable
        # (locked db, disk full): the poller's never-raises guarantee holds
        logger.error("failed to record poll failure for mailbox %s: %s", mailbox_id, record_error)
