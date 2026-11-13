"""Title generation: OCR content excerpt + filename -> concise document title."""

import logging

from papra_suite.llm_client import LLMClient, LLMError

logger = logging.getLogger(__name__)

TITLE_MAX_LENGTH = 80
EXCERPT_CHARS = 2000

TITLE_SYSTEM_PROMPT = (
    "You generate concise titles for archived documents. Output only the "
    "title: one line, no quotes, maximum 80 characters, in the document's "
    "language (German or English)."
)


def build_title_messages(*, filename: str, excerpt: str) -> list[dict]:
    trimmed = excerpt[:EXCERPT_CHARS]
    return [
        {"role": "system", "content": TITLE_SYSTEM_PROMPT},
        {
            "role": "user",
            "content": (
                f"Filename: {filename}\n\n"
                f"Document content excerpt:\n\n{trimmed}\n\n"
                "Generate the title."
            ),
        },
    ]


def sanitize_title(raw: str, *, fallback: str = "Untitled") -> str:
    title = raw.strip().strip("\"'`").strip()
    title = " ".join(title.split())  # collapse whitespace/newlines to one line
    if len(title) > TITLE_MAX_LENGTH:
        title = title[:TITLE_MAX_LENGTH].rstrip()
    return title or fallback


async def generate_title(llm: LLMClient, *, filename: str, excerpt: str) -> str:
    """Generate a title; on LLM failure, fall back to the filename."""
    try:
        raw = await llm.chat(build_title_messages(filename=filename, excerpt=excerpt))
    except LLMError as error:
        logger.warning("title generation failed (%s); falling back to filename", error)
        return sanitize_title(filename, fallback=filename)
    return sanitize_title(raw, fallback=sanitize_title(filename, fallback=filename))
