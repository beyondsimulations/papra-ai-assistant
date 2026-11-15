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
from dataclasses import dataclass, field
from email import message_from_bytes
from email.header import decode_header, make_header
from email.message import Message

from papra_suite.allowlist import AMBIGUOUS_FROM, DENY_MALFORMED, PERMISSION, AllowList
from papra_suite.logging_setup import SECRET_REGISTRY
from papra_suite.papra_client import PapraClient, PapraError
from papra_suite.pipeline import enqueue_ocr_job
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
    # (uid, verdict) for messages refused on their headers alone: the body was
    # never requested, so a refused sender costs a header read, not a download
    # plus a MIME parse.
    denied: list[tuple[int, str]] = field(default_factory=list)


@dataclass
class ParsedMessage:
    message_id: str | None
    subject: str | None
    pdf_attachments: list[Attachment] = field(default_factory=list)
    skipped_attachments: list[str] = field(default_factory=list)
    # The raw From: header text, NOT a parsed address: the allow-list parses it
    # and decides what counts as malformed. Duplicated From: fields are joined
    # so the parser sees two addresses and calls it malformed rather than
    # picking whichever identity the attacker happened to offer.
    from_header: str | None = None


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
    # Exactly one From field or there is nothing to adjudicate: duplicate
    # fields are ambiguous by construction and are never joined into one string
    # for the parser to resolve at its leisure.
    from_fields = [v for k, v in message.items() if k.lower() == "from"]
    parsed = ParsedMessage(
        message_id=message.get("Message-ID"),
        subject=_decode_header_value(message.get("Subject")),
        from_header=from_fields[0] if len(from_fields) == 1 else AMBIGUOUS_FROM,
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


def dedup_hash(mailbox_id: str, message_id: str | None, filename: str, size: int) -> str:
    basis = f"{mailbox_id}\x00{message_id or ''}\x00{filename}\x00{size}"
    return hashlib.sha256(basis.encode()).hexdigest()


def compute_file_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


VANISHED = object()  # validated absence, not "could not read"


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

    def fetch_since(self, last_seen_uid: int | None, admit=None) -> FetchBatch:
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
            if size is VANISHED:
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
            if admit is not None:
                # Decide on the headers BEFORE asking for the body. The
                # alternative — download 20 MB, parse it, then notice the
                # sender is not allowed — is the cost this ordering exists to
                # remove, and it is the difference between a junk folder being
                # cheap to refuse and being a bandwidth bill.
                header = self._fetch_literal(uid, "[HEADER]")
                if header is None:
                    logger.warning("message uid=%s vanished before header fetch;"
                                   " skipping", uid)
                    continue
                verdict = admit(header)
                if verdict != PERMISSION:
                    logger.info("message uid=%s refused on its headers; body not"
                                " requested", uid)
                    batch.denied.append((uid, verdict))
                    continue
            raw = self._fetch_literal(uid, "[]")
            if raw is None:
                logger.warning("message uid=%s vanished before fetch; skipping", uid)
                continue
            batch.messages.append((uid, raw))
        return batch

    def _fetch_literal(self, uid: int, section: str) -> bytes | None:
        """The literal for one uid, or None only if the message is GONE.

        "I could not read your answer" and "that message no longer exists" are
        different facts. The earlier code collapsed them into a skip, and a skip
        is safe only when the message is really gone: if any later message in the
        same poll succeeds, the cursor advances past the skipped uid and the
        skipped one is never searched again — mail lost with no error anywhere.
        So an unreadable shape is a failure that aborts the batch and keeps the
        cursor where it is, and it is retried next poll. A permanently unreadable
        message will eventually disable the mailbox through the failure path,
        which is the right outcome: a visible outage beats a quiet hole.
        """
        status, fetch_data = self._imap.uid("FETCH", str(uid), f"(BODY.PEEK{section})")
        if status != "OK":
            raise imaplib.IMAP4.error(f"UID FETCH failed for {uid}")
        entry = fetch_data[0] if fetch_data else None
        if isinstance(entry, tuple) and len(entry) >= 2 and isinstance(entry[1], bytes):
            prefix = entry[0] if isinstance(entry[0], bytes) else b""
            described = prefix.decode("ascii", errors="replace")
            # The response must describe the uid that was asked for: a literal
            # for another message would otherwise be ingested, deduplicated and
            # retired under this uid.
            if not re.search(rf"\bUID {uid}\b", described):
                raise imaplib.IMAP4.error(
                    f"UID FETCH for {uid} returned a response describing another "
                    f"message ({described[:60]!r}); refusing to ingest it as {uid}"
                )
            return entry[1]
        # No literal: benign if the message was expunged under us, and the
        # metadata probe is the cheapest way to ask. Anything else — PARTIAL,
        # untagged-only, a shape this client does not understand — means the
        # message is still there and we did not read it.
        if self._rfc822_size(uid) is VANISHED:
            return None
        raise imaplib.IMAP4.error(
            f"UID FETCH for {uid} returned no message data while the message is "
            "still present; not skipping it, because that message would never be "
            "read"
        )

    def _rfc822_size(self, uid: int) -> int | object:
        """The message's RFC822.SIZE, or VANISHED — never both meanings at once.

        Returning None for "gone" and also for "I could not read that" was the
        same bug one layer down: every caller read None as "skip it", so a
        malformed metadata response let the cursor walk past a message that was
        never decided. Absence has to be VALIDATED, not inferred from confusion.
        """
        status, fetch_data = self._imap.uid("FETCH", str(uid), "(RFC822.SIZE)")
        if status != "OK":
            raise imaplib.IMAP4.error(f"UID FETCH failed for {uid}")
        entry = fetch_data[0] if fetch_data else None
        if entry is None or entry == [None] or entry == b"":
            return VANISHED
        if not isinstance(entry, bytes):
            raise imaplib.IMAP4.error(
                f"UID FETCH for {uid} returned unreadable metadata "
                f"({entry!r:.60}); not treating that as a deleted message")
        if not re.search(rf"\bUID {uid}\b", entry.decode("ascii", errors="replace")):
            raise imaplib.IMAP4.error(
                f"UID FETCH for {uid} returned metadata describing another message "
                f"({entry!r:.60})")
        match = re.search(rb"RFC822\.SIZE\s+(\d+)", entry)
        if match is None:
            raise imaplib.IMAP4.error(
                f"UID FETCH for {uid} carried no RFC822.SIZE; refusing to guess "
                "whether the message exists")
        return int(match.group(1))

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
    # appended last, same reason: sender admission is a normal outcome, and it
    # is counted so "why is my invoice missing?" is a query rather than an
    # archaeology dig through logs
    senders_denied: int = 0
    malformed_senders: int = 0
    # True when the allow-list holds no usable entry: nothing was fetched,
    # nothing was ingested and nothing was retired, because pausing on a
    # half-configured gate must not consume the inbox.
    ingestion_paused: bool = False


async def poll_mailbox_once(store: Store, client: PapraClient,
                            mailbox: Mailbox, org: Org, *,
                            allowlist: AllowList) -> PollResult:
    """Poll one mailbox once. Never raises: failures land in the result and
    the mailbox's persisted error state (3 consecutive -> disabled).

    There is no per-mailbox subject filter: the dedicated-inbox design moves
    that decision in front of the software, where a human forwards what
    belongs in Papra. So every message's supported attachments are ingested,
    and what this function owes is reliability rather than judgement — upload
    before retire, the cursor-prefix rule, cancellation that marks nothing, and
    a failure path that is visible instead of quiet.
    """
    return await _poll_mailbox_once(store, client, mailbox, org,
                         allowlist=allowlist)


async def _poll_mailbox_once(store: Store, client: PapraClient, mailbox: Mailbox,
                             org: Org, *, allowlist: AllowList) -> PollResult:
    SECRET_REGISTRY.add(mailbox.password)
    session = MailboxSession(
        host=mailbox.host, port=mailbox.port, username=mailbox.username,
        password=mailbox.password, ssl_mode=mailbox.ssl_mode,
    )
    result = PollResult()
    if allowlist.paused:
        # No usable entry, so nothing about any message is decided — and an
        # undecided gate must not fetch, ingest or RETIRE anything. Marking mail
        # seen while the operator is still writing the configuration would
        # quietly consume the inbox, which is the one thing this branch exists
        # to prevent. It is deliberately not a poll failure either: a
        # half-configured mailbox must not be disabled by the 3-strike path.
        result.ingestion_paused = True
        logger.warning("ingest paused for mailbox %s: %s", mailbox.id,
                       allowlist.describe())
        return result
    try:
        await asyncio.to_thread(session.connect)
        uidvalidity = await asyncio.to_thread(session.select_inbox)
        rescan = mailbox.uidvalidity is not None and mailbox.uidvalidity != uidvalidity
        last_seen = None if (mailbox.last_seen_uid is None or rescan) else mailbox.last_seen_uid
        # Admission is handed to the fetch so a refused sender is decided on its
        # headers: the policy stays here, the transport stays in the session, and
        # what the session is told is a question about bytes, not a rule.
        batch = await asyncio.to_thread(session.fetch_since, last_seen,
                                        allowlist.admit_headers)
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
        # merge messages, oversized uids and header-stage refusals back into uid
        # order so the freeze-on-failure semantics stay position-correct
        entries = sorted(
            [(uid, 0, raw) for uid, raw in batch.messages]
            + [(uid, 1, None) for uid in batch.oversized_uids]
            + [(uid, 2, verdict) for uid, verdict in batch.denied]
        )

        def retire_refused(refused_uid: int, verdict: str) -> None:
            # A rejected sender is a NORMAL outcome, not a fault. The message is
            # retired and the cursor advances, so a busy junk folder cannot
            # re-fetch and re-reject the same uids on every poll — and the poll
            # itself must not fail, because three consecutive failures disable a
            # mailbox and unsolicited mail must never be able to do that.
            nonlocal candidate_last_seen
            if verdict == DENY_MALFORMED:
                result.malformed_senders += 1
                logger.warning("message uid=%s ignored: its From: header does not "
                               "resolve to exactly one sender", refused_uid)
            else:
                result.senders_denied += 1
                logger.info("message uid=%s ignored: sender is not on the allow-list",
                            refused_uid)
            seen_uids.append(refused_uid)
            if not prefix_failed:
                candidate_last_seen = refused_uid

        for uid, kind, payload in entries:
            result.messages_scanned += 1
            if kind == 2:
                # refused on its headers; its body was never requested
                retire_refused(uid, payload)
                continue
            raw = payload
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
            verdict = allowlist.verdict_for_header(parsed.from_header)
            if verdict != PERMISSION:
                # Defence in depth: the fetch already refused on the headers, so
                # reaching here means a caller handed this loop a body that was
                # never admitted, or a header that reads differently once parsed.
                # Either way the answer is the same refusal, not a new path.
                retire_refused(uid, verdict)
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
    if result.senders_denied or result.malformed_senders:
        # One line per poll, so "where is my invoice?" is answered by a count
        # and a cursor rather than by reading a page of per-message logs — and
        # the counts are the recovery breadcrumb, since the message itself is
        # still on the server and can be replayed after the list is corrected.
        logger.info("poll summary: %d scanned, %d uploaded, %d denied by sender "
                    "allow-list, %d with an unresolvable sender",
                    result.messages_scanned, result.documents_uploaded,
                    result.senders_denied, result.malformed_senders)
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
