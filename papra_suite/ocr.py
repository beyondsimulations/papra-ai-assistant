"""OCR: PDF -> page images -> (vision model, called by the worker) -> markdown."""

import logging

import fitz

logger = logging.getLogger(__name__)

OCR_SYSTEM_PROMPT = (
    "Transcribe this document page to clean markdown. Preserve structure: "
    "headings, lists, tables. Output only the markdown. Text is German or English."
)


def page_count(pdf_bytes: bytes) -> int:
    """Return the PDF's page count without rendering anything."""
    with fitz.open(stream=pdf_bytes, filetype="pdf") as pdf:
        return pdf.page_count


def render_page(pdf_bytes: bytes, index: int, *, dpi: int,
                pixel_budget: int) -> bytes:
    """Render ONE page as a PNG at dpi, downscaling if the render would
    exceed pixel_budget. Single-page so callers can render lazily and hold
    at most one page image in memory at a time."""
    with fitz.open(stream=pdf_bytes, filetype="pdf") as pdf:
        page = pdf[index]
        zoom = dpi / 72.0
        rect = page.rect
        base_pixels = rect.width * zoom * rect.height * zoom
        if base_pixels > pixel_budget:
            zoom *= (pixel_budget / base_pixels) ** 0.5
        pixmap = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom))
        return pixmap.tobytes("png")


def render_pages(pdf_bytes: bytes, *, dpi: int, max_pages: int,
                 pixel_budget: int) -> list[bytes]:
    """Render up to max_pages pages as PNGs at dpi, downscaling any page
    whose render would exceed pixel_budget. Thin wrapper over render_page."""
    total = page_count(pdf_bytes)
    pages = [
        render_page(pdf_bytes, index, dpi=dpi, pixel_budget=pixel_budget)
        for index in range(min(total, max_pages))
    ]
    if total > max_pages:
        logger.warning(
            "document has %d pages; skipped %d additional pages beyond "
            "max_pages=%d",
            total, total - max_pages, max_pages,
        )
    return pages


def assemble_markdown(page_markdowns: list[str]) -> str:
    return "\n\n".join(page_markdowns)
