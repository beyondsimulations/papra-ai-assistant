"""Minimal hand-written one-page PDFs with a text layer.

Same shape as `../2026-11-17-kreuzakt/sample.pdf` (the file that proved a
hand-written PDF is enough for keyless extraction), but parameterised so a probe
can produce N *distinct* files: cross-object dedupe is by file hash, so a probe
that needs N documents must produce N different byte strings. The marker is
ASCII on purpose — umlaut handling was measured by the 2026-11-17 probe and is
not what this probe asks about.
"""


def build_pdf(marker: str) -> bytes:
    lines = [f"PAPRA-CONTRACT-PROBE {marker}", "Rechnung Nr. 4711", "Miete Oktober 2025"]
    stream = "BT /F1 24 Tf 60 720 Td (" + lines[0] + ") Tj"
    for line in lines[1:]:
        stream += " 0 -30 Td (" + line + ") Tj"
    stream += " ET\n"
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length " + str(len(stream.encode())).encode() + b" >>\nstream\n"
        + stream.encode()
        + b"endstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]

    out = bytearray(b"%PDF-1.4\n")
    offsets = []
    for number, body in enumerate(objects, start=1):
        offsets.append(len(out))
        out += f"{number} 0 obj\n".encode() + body + b"\nendobj\n"

    xref_at = len(out)
    out += f"xref\n0 {len(objects) + 1}\n".encode()
    out += b"0000000000 65535 f \n"
    for offset in offsets:
        out += f"{offset:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_at}\n%%EOF\n".encode()
    return bytes(out)
