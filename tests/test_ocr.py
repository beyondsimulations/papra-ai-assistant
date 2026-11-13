import fitz

from papra_suite.ocr import (
    OCR_SYSTEM_PROMPT,
    assemble_markdown,
    render_page,
    render_pages,
)


def make_pdf(pages: list[str]) -> bytes:
    pdf = fitz.open()
    for text in pages:
        page = pdf.new_page()
        page.insert_text((72, 72), text)
    data = pdf.tobytes()
    pdf.close()
    return data


def test_prompt_is_the_spec_wording():
    assert OCR_SYSTEM_PROMPT == (
        "Transcribe this document page to clean markdown. Preserve structure: "
        "headings, lists, tables. Output only the markdown. Text is German or English."
    )


def test_render_pages_returns_png_per_page():
    pages = render_pages(make_pdf(["Seite eins", "Seite zwei"]), dpi=72,
                        max_pages=50, pixel_budget=2_000_000)
    assert len(pages) == 2
    assert all(png.startswith(b"\x89PNG") for png in pages)


def test_render_pages_caps_and_warns(caplog):
    pdf = make_pdf([f"page {i}" for i in range(5)])
    pages = render_pages(pdf, dpi=72, max_pages=3, pixel_budget=2_000_000)
    assert len(pages) == 3
    # pin the exact skipped count — an always-true "skipped" arm would
    # match the format string even if the count were wrong
    assert any("2 additional pages" in record.getMessage().lower()
               for record in caplog.records)


def test_render_pages_downscales_over_pixel_budget():
    pdf = make_pdf(["dense"])  # A4 at 150 dpi is ~1.75M pixels; use a tiny budget
    budget = 50_000
    pages = render_pages(pdf, dpi=150, max_pages=1, pixel_budget=budget)
    assert len(pages) == 1
    pixmap = fitz.Pixmap(pages[0])  # PIXEL dims (page.rect would re-scale by dpi)
    # pin the sqrt downscale FACTOR: the render must land ON the budget
    # (within rounding), not merely under it. Linear scaling would render
    # budget^2/base_pixels (~1.1k) pixels — far below the lower bound.
    # Mutation: `** 0.5` -> linear -> this lower bound fails.
    assert budget * 0.85 <= pixmap.width * pixmap.height <= budget * 1.05


def test_render_page_scales_the_raster_with_the_configured_dpi():
    """dpi pin for the live render helper: kills `zoom = dpi / 72.0` ->
    `zoom = 1.0`. The only other dpi-sensitive test normalises its expectation
    to the pixel budget, so a dpi-blind render still lands exactly on the
    budget and passes; here the budget is deliberately huge, so the raster size
    can only come from the dpi (a dpi-blind render ships 72 dpi images to the
    vision model and dense pages silently degrade)."""
    pdf = make_pdf(["dense"])
    small = fitz.Pixmap(render_page(pdf, 0, dpi=72, pixel_budget=10**12))
    big = fitz.Pixmap(render_page(pdf, 0, dpi=144, pixel_budget=10**12))
    assert big.width > small.width * 1.8  # 2x dpi -> ~2x linear resolution
    assert big.height > small.height * 1.8
    assert big.width * big.height < 10**12  # the budget was not the limiter


def test_render_pages_empty_pdf():
    # this PyMuPDF cannot save a zero-page document, so handcraft a minimal
    # valid PDF with no pages instead of fitz.open() + tobytes()
    data = (
        b"%PDF-1.4\n"
        b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n"
        b"2 0 obj\n<< /Type /Pages /Kids [] /Count 0 >>\nendobj\n"
        b"trailer\n<< /Root 1 0 R /Size 3 >>\n"
    )
    assert render_pages(data, dpi=72, max_pages=10, pixel_budget=2_000_000) == []


def test_assemble_markdown_joins_pages():
    assert assemble_markdown(["# A", "# B"]) == "# A\n\n# B"
    assert assemble_markdown([]) == ""
