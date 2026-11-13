from email.message import EmailMessage

from papra_suite.mail import (
    compute_file_hash,
    dedup_hash,
    parse_message,
    sanitize_filename,
    sniff_is_pdf,
    subject_matches,
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


def test_parse_message_without_message_id():
    raw = build_raw(message_id=None)
    parsed = parse_message(raw)
    assert parsed.message_id is None


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


def test_subject_matches():
    assert subject_matches("Rechnung 42", None) is True
    assert subject_matches("Rechnung 42", "") is True
    assert subject_matches("Rechnung 42", ".*rechnung.*") is True
    assert subject_matches("Spam", ".*rechnung.*") is False
    assert subject_matches(None, ".*rechnung.*") is False


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
