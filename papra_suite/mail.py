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
from dataclasses import dataclass, field
from email import message_from_bytes
from email.header import decode_header, make_header
from email.message import Message

from papra_suite.logging_setup import SECRET_REGISTRY
from papra_suite.papra_client import PapraClient, PapraError
from papra_suite.pipeline import enqueue_ocr_job
from papra_suite.store import Mailbox, Org, Store

logger = logging.getLogger(__name__)

MAX_FILENAME_LENGTH = 120
PDF_MAGIC = b"%PDF-"


@dataclass
class Attachment:
    filename: str
    content: bytes


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
        if payload is None:
            continue
        filename = part.get_filename()
        if filename is None:
            continue  # body text or inline part without a name: not an attachment
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


def subject_matches(subject: str | None, pattern: str | None) -> bool:
    """Optional per-mailbox subject regex filter; no pattern means match all."""
    if not pattern:
        return True
    if subject is None:
        return False
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
        if self._ssl_mode == "ssl":
            self._imap = imaplib.IMAP4_SSL(self._host, self._port)
        elif self._ssl_mode == "starttls":
            self._imap = imaplib.IMAP4(self._host, self._port)
            self._imap.starttls()
        else:
            self._imap = imaplib.IMAP4(self._host, self._port)
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

    def fetch_since(self, last_seen_uid: int | None) -> list[tuple[int, bytes]]:
        if last_seen_uid is None:
            status, data = self._imap.uid("SEARCH", None, "ALL")
        else:
            status, data = self._imap.uid("SEARCH", None, f"UID {last_seen_uid + 1}:*")
        if status != "OK":
            raise imaplib.IMAP4.error("UID SEARCH failed")
        uids = [int(u) for u in data[0].split()]
        uids = [u for u in uids if last_seen_uid is None or u > last_seen_uid]
        uids.sort()
        messages = []
        for uid in uids:
            status, fetch_data = self._imap.uid("FETCH", str(uid), "(BODY.PEEK[])")
            if status != "OK":
                raise imaplib.IMAP4.error(f"UID FETCH failed for {uid}")
            raw = fetch_data[0][1]
            messages.append((uid, raw))
        return messages

    def mark_seen(self, uids: list[int]) -> None:
        for uid in uids:
            self._imap.uid("STORE", str(uid), "+FLAGS", "(\\Seen)")

    def delete_and_expunge(self, uids: list[int]) -> None:
        for uid in uids:
            self._imap.uid("STORE", str(uid), "+FLAGS", "(\\Deleted)")
        self._imap.expunge()

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


async def poll_mailbox_once(store: Store, client: PapraClient,
                            mailbox: Mailbox, org: Org) -> PollResult:
    """Poll one mailbox once. Never raises: failures land in the result and
    the mailbox's persisted error state (3 consecutive -> disabled)."""
    SECRET_REGISTRY.add(mailbox.password)
    session = MailboxSession(
        host=mailbox.host, port=mailbox.port, username=mailbox.username,
        password=mailbox.password, ssl_mode=mailbox.ssl_mode,
    )
    result = PollResult()
    try:
        await asyncio.to_thread(session.connect)
        uidvalidity = await asyncio.to_thread(session.select_inbox)
        rescan = mailbox.uidvalidity is not None and mailbox.uidvalidity != uidvalidity
        last_seen = None if (mailbox.last_seen_uid is None or rescan) else mailbox.last_seen_uid
        raw_messages = await asyncio.to_thread(session.fetch_since, last_seen)
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
        for uid, raw in raw_messages:
            result.messages_scanned += 1
            parsed = parse_message(raw)
            if not subject_matches(parsed.subject, mailbox.subject_filter):
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
    last_seen = candidate_last_seen if candidate_last_seen is not None \
        else (0 if rescan else (mailbox.last_seen_uid or 0))
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
