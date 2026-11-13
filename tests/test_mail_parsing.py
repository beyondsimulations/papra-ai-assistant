from email.message import EmailMessage
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pytest

from papra_suite.mail import (
    Attachment,
    compute_file_hash,
    dedup_hash,
    parse_message,
    sanitize_filename,
    sniff_is_pdf,
    subject_matches,
    validate_subject_filter,
)

PDF_BYTES = b"%PDF-1.4 fake pdf body"


def build_raw(*, subject="Invoice", message_id="<msg-1@example.com>", attachments=()):
    message = EmailMessage()
    message["From"] = "sender@example.com"
    message["To"] = "box@example.com"
    message["Subject"] = subject
    if message_id:
        message["Message-ID"] = message_id
    for filename, content, mime in attachments:
        message.add_attachment(content, maintype=mime[0], subtype=mime[1], filename=filename)
    return message.as_bytes()


def test_sniff_is_pdf_ignores_declared_mime():
    assert sniff_is_pdf(PDF_BYTES)
    assert sniff_is_pdf(b"%PDF-")
    assert not sniff_is_pdf(b"PK\x03\x04 zip")
    assert not sniff_is_pdf(b"")
    assert not sniff_is_pdf(b"%PD")  # truncated magic


def test_sniff_is_pdf_catches_mislabeled_pdf():
    # declared as text/plain but actually a PDF -> must be treated as PDF
    raw = build_raw(attachments=[("a.pdf", PDF_BYTES, ("text", "plain"))])
    parsed = parse_message(raw)
    assert [a.filename for a in parsed.pdf_attachments] == ["a.pdf"]
    assert parsed.skipped_attachments == []


def test_parse_message_extracts_metadata_and_attachments():
    raw = build_raw(attachments=[
        ("rechnung.pdf", PDF_BYTES, ("application", "pdf")),
        ("photo.jpg", b"\xff\xd8 jpeg", ("image", "jpeg")),
    ])
    parsed = parse_message(raw)
    assert parsed.message_id == "<msg-1@example.com>"
    assert parsed.subject == "Invoice"
    assert [a.filename for a in parsed.pdf_attachments] == ["rechnung.pdf"]
    assert parsed.pdf_attachments[0].content == PDF_BYTES
    assert len(parsed.skipped_attachments) == 1
    assert "photo.jpg" in parsed.skipped_attachments[0]


def test_parse_message_decodes_rfc2047_subject():
    # non-ASCII subjects are RFC 2047 encoded on the wire; the subject filter
    # must see the decoded string or it false-negatives every German subject
    raw = build_raw(subject="Rechnung über Möbel")
    parsed = parse_message(raw)
    assert parsed.subject == "Rechnung über Möbel"
    assert subject_matches(parsed.subject, ".*über.*") is True


def test_parse_message_decodes_rfc2047_filenames():
    # non-ASCII attachment filenames are RFC 2047 encoded on the wire; the
    # encoded word must not survive into the (Papra document / M3 title)
    # filename — decode before sanitize, same ruling as the subject
    raw = build_raw(attachments=[
        ("Rechnung 2026 – November.pdf", PDF_BYTES, ("application", "pdf")),
        ("Foto – Dezember.jpg", b"\xff\xd8jpeg", ("image", "jpeg")),
    ])
    parsed = parse_message(raw)
    assert [a.filename for a in parsed.pdf_attachments] == ["Rechnung 2026 – November.pdf"]
    assert parsed.skipped_attachments == ["Foto – Dezember.jpg"]
    for name in [a.filename for a in parsed.pdf_attachments] + parsed.skipped_attachments:
        assert "=?utf-8" not in name


def test_parse_message_survives_malformed_encoded_words():
    # a malformed encoded word must never raise out of parse_message: a
    # poison message would fail EVERY poll on the same uid (mailbox disabled
    # after 3 consecutive failures). Unknown charset -> LookupError, broken
    # base64 -> HeaderParseError: both must land in the raw-string fallback.
    raw = (
        b"From: sender@example.com\r\n"
        b"To: box@example.com\r\n"
        b"Message-ID: <poison@example.com>\r\n"
        b"Subject: =?unknown-charset-x?Q?hello?=\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: multipart/mixed; boundary=\"pin-bound\"\r\n"
        b"\r\n"
        b"--pin-bound\r\n"
        b"Content-Type: application/pdf\r\n"
        b"Content-Disposition: attachment;\r\n"
        b" filename=\"=?utf-8?B?not-base64!!?=\"\r\n"
        b"Content-Transfer-Encoding: 7bit\r\n"
        b"\r\n"
        + PDF_BYTES +
        b"\r\n--pin-bound--\r\n"
    )

    parsed = parse_message(raw)

    # the fallback returns the RAW string in each position, no exception
    assert parsed.subject == "=?unknown-charset-x?Q?hello?="
    assert [a.filename for a in parsed.pdf_attachments] == ["=?utf-8?B?not-base64!!?="]
    assert parsed.pdf_attachments[0].content == PDF_BYTES


def test_parse_message_without_message_id():
    raw = build_raw(message_id=None)
    parsed = parse_message(raw)
    assert parsed.message_id is None


def test_parse_message_recovers_filename_less_pdf_part():
    # a filename-less application/pdf part (some forwarders strip the name)
    # must not be silently dropped: sniff the payload and classify it as an
    # attachment under the fallback name. A plain text body never starts with
    # %PDF- and still lands in neither list.
    outer = MIMEMultipart()
    outer["Subject"] = "Invoice"
    outer.attach(MIMEText("please find the invoice attached", "plain"))
    outer.attach(MIMEApplication(PDF_BYTES, _subtype="pdf"))  # deliberately unnamed

    parsed = parse_message(outer.as_bytes())

    assert parsed.pdf_attachments == [Attachment("document.pdf", PDF_BYTES)]
    assert parsed.skipped_attachments == []


def test_parse_message_skips_named_eml_but_extracts_embedded_pdf():
    # a NAMED message/rfc822 part (.eml forward) has no decodable payload:
    # per spec it must be logged and skipped (not silently dropped into
    # neither list), while the walk still descends and extracts the PDF
    # attached inside the forwarded mail
    inner = EmailMessage()
    inner["From"] = "inner@example.com"
    inner["To"] = "box@example.com"
    inner["Subject"] = "Inner invoice"
    inner["Message-ID"] = "<inner@x>"
    inner.add_attachment(PDF_BYTES, maintype="application", subtype="pdf",
                         filename="inner.pdf")

    raw = (
        b"From: sender@example.com\r\n"
        b"To: box@example.com\r\n"
        b"Subject: Fwd: Invoice\r\n"
        b"MIME-Version: 1.0\r\n"
        b'Content-Type: multipart/mixed; boundary="OUTER-BOUNDARY"\r\n'
        b"\r\n"
        b"--OUTER-BOUNDARY\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"\r\nsee the forwarded mail\r\n"
        b"--OUTER-BOUNDARY\r\n"
        b'Content-Type: message/rfc822; name="forward.eml"\r\n'
        b'Content-Disposition: attachment; filename="forward.eml"\r\n'
        b"\r\n"
        + inner.as_bytes() + b"\r\n"
        b"--OUTER-BOUNDARY--\r\n"
    )

    parsed = parse_message(raw)

    assert parsed.skipped_attachments == ["forward.eml"]
    assert [a.filename for a in parsed.pdf_attachments] == ["inner.pdf"]
    assert parsed.pdf_attachments[0].content == PDF_BYTES


def test_parse_message_plain_body_only():
    raw = build_raw()
    parsed = parse_message(raw)
    assert parsed.pdf_attachments == [] and parsed.skipped_attachments == []


def test_sanitize_filename_strips_paths_and_control_chars():
    assert sanitize_filename("../../etc/passwd.pdf") == "passwd.pdf"
    assert sanitize_filename("folder\\evil.pdf") == "evil.pdf"
    assert sanitize_filename("bad\x00name.pdf") == "badname.pdf"
    assert sanitize_filename("x" * 300 + ".pdf").endswith(".pdf")
    assert len(sanitize_filename("x" * 300 + ".pdf")) <= 120
    assert sanitize_filename(None) == "document.pdf"
    assert sanitize_filename("") == "document.pdf"
    assert sanitize_filename("   .pdf") == "document.pdf"


def test_sanitize_filename_caps_length_without_short_suffix():
    # >120 chars with NO extension: the else branch truncates to the cap —
    # dropping it would keep the full (300-char) name
    assert len(sanitize_filename("x" * 300)) == 120
    # an extension LONGER than 10 chars also takes the else arm (the short-
    # suffix branch would produce a 120+20+1-char name)
    assert len(sanitize_filename("x" * 200 + "." + "y" * 20)) == 120


def test_subject_matches():
    assert subject_matches("Rechnung 42", None) is True
    assert subject_matches("Rechnung 42", "") is True
    assert subject_matches("Rechnung 42", ".*rechnung.*") is True
    assert subject_matches("Spam", ".*rechnung.*") is False
    assert subject_matches(None, ".*rechnung.*") is False


def test_validate_subject_filter_rejects_catastrophic_pattern():
    # the classic exponential-backtracking class: the t("a"*30)/t("a"*20)
    # growth ratio is ~100x, far past the 10x threshold
    with pytest.raises(ValueError, match="unsafe"):
        validate_subject_filter(r"(a|aa)+b")


def test_validate_subject_filter_accepts_linear_and_empty():
    validate_subject_filter(None)  # no filter at all
    validate_subject_filter("")  # empty filter: match-all
    validate_subject_filter(r".*rechnung.*")  # linear scan: ratio ~1x


def test_validate_subject_filter_rejects_invalid_regex():
    # an uncompilable pattern must surface as ValueError (the poll-failure
    # path), not as a raw re.error mid-poll
    with pytest.raises(ValueError, match="invalid subject filter"):
        validate_subject_filter("(")


def test_dedup_hash_is_stable_and_discriminating():
    a = dedup_hash("mb1", "<m@x>", "f.pdf", 100)
    assert a == dedup_hash("mb1", "<m@x>", "f.pdf", 100)
    assert a != dedup_hash("mb2", "<m@x>", "f.pdf", 100)
    assert a != dedup_hash("mb1", "<m@y>", "f.pdf", 100)
    assert a != dedup_hash("mb1", "<m@x>", "g.pdf", 100)
    assert a != dedup_hash("mb1", "<m@x>", "f.pdf", 101)
    assert a != dedup_hash("mb1", None, "f.pdf", 100)


def test_compute_file_hash():
    assert compute_file_hash(b"abc") == compute_file_hash(b"abc")
    assert compute_file_hash(b"abc") != compute_file_hash(b"abd")
    assert len(compute_file_hash(b"abc")) == 64
