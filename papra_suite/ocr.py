"""OCR: PDF -> page images -> (vision model, called by the worker) -> markdown."""

import logging

import fitz

logger = logging.getLogger(__name__)

OCR_SYSTEM_PROMPT = (
    "Transcribe this document page to clean markdown. Preserve structure: "
    "headings, lists, tables. Output only the markdown. Text is German or English."
)


def render_pages(pdf_bytes: bytes, *, dpi: int, max_pages: int,
                 pixel_budget: int) -> list[bytes]:
    """Render up to max_pages pages as PNGs at dpi, downscaling any page
    whose render would exceed pixel_budget."""
    pages: list[bytes] = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as pdf:
        total = pdf.page_count
        for index in range(min(total, max_pages)):
            page = pdf[index]
            zoom = dpi / 72.0
            rect = page.rect
            base_pixels = rect.width * zoom * rect.height * zoom
            if base_pixels > pixel_budget:
                zoom *= (pixel_budget / base_pixels) ** 0.5
            pixmap = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom))
            pages.append(pixmap.tobytes("png"))
        if total > max_pages:
            logger.warning(
                "document has %d pages; skipped %d additional pages beyond "
                "max_pages=%d",
                total, total - max_pages, max_pages,
            )
    return pages


def assemble_markdown(page_markdowns: list[str]) -> str:
    return "\n\n".join(page_markdowns)
