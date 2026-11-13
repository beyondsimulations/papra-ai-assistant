"""Thin async client for OpenAI-compatible chat-completions endpoints.

Mirrors papra_client's retry policy: 3 attempts, exponential backoff,
retry on 429 + all 5xx + network errors; other 4xx fail permanently.
A 200 whose finish_reason says the generation stopped early is a failure
too — it is raised as a permanent LLMError, never returned as content.
"""

import asyncio
import base64
import logging

import httpx

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3


class LLMError(Exception):
    def __init__(self, message: str, status: int | None = None, *,
                 permanent: bool = False) -> None:
        super().__init__(message)
        self.status = status
        # True when retrying cannot help — a completion that stopped early at
        # the output cap repeats identically, so backing off only delays the
        # same half page. Plain 4xx statuses need no flag: worker_loop tests
        # them via .status.
        self.permanent = permanent


class LLMClient:
    def __init__(self, base_url: str, api_key: str, model: str, *,
                 timeout_s: float = 300.0, backoff_base_s: float = 0.5,
                 transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._model = model
        self._backoff_base_s = backoff_base_s
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout_s,
            transport=transport,
        )

    async def __aenter__(self) -> "LLMClient":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def chat(self, messages: list[dict], *, image: bytes | None = None) -> str:
        payload_messages = [dict(message) for message in messages]
        if image is not None:
            # the LAST user message carries the image content part
            for message in reversed(payload_messages):
                if message["role"] == "user":
                    data_url = "data:image/png;base64," + base64.b64encode(image).decode()
                    message["content"] = [
                        {"type": "text", "text": message["content"]},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ]
                    break
            else:
                raise LLMError("image given but no user message to attach it to")
        body = {"model": self._model, "messages": payload_messages}

        last_error = ""
        for attempt in range(MAX_ATTEMPTS):
            try:
                # The standard OpenAI-compatible route, /v1-namespaced: vLLM,
                # Ollama and LM Studio document only this path (mlx_lm.server
                # maps both). Kept as a literal so the test fake cannot be
                # wrong together with the client via a shared constant.
                response = await self._client.post("/v1/chat/completions", json=body)
            except httpx.TransportError as error:
                last_error = str(error)
                if attempt == MAX_ATTEMPTS - 1:
                    break
                await asyncio.sleep(self._backoff_base_s * 4**attempt)
                continue
            if (response.status_code == 429 or response.status_code >= 500) \
                    and attempt < MAX_ATTEMPTS - 1:
                await asyncio.sleep(self._backoff_base_s * 4**attempt)
                continue
            if response.is_success:
                try:
                    choice = response.json()["choices"][0]
                    # .get("content"), NOT ["content"].strip(): a server that
                    # hit its output cap answers
                    # {"content": null, "finish_reason": "length"}, and a raise
                    # here would report that as a RETRYABLE malformed body
                    # (status 200, permanent=False) — neither the 4xx rule nor
                    # the permanence flag would apply, and a deterministic
                    # truncation would ride all three attempts (three identical
                    # vision calls plus ~2.5 min of backoff) instead of failing
                    # once. Read it, then let the finish_reason gate decide.
                    content = choice["message"].get("content")
                    finish_reason = choice.get("finish_reason")
                except Exception as error:  # noqa: BLE001
                    # A 2xx body is not guaranteed to be a well-formed chat
                    # completions payload — it may be non-UTF-8 (UnicodeDecodeError)
                    # or an unexpected shape (TypeError for a string message or a
                    # top-level array). Surface ANY parse failure as LLMError (not
                    # a raw decode/type error) so callers' never-raises/fallback
                    # contracts hold. Only the parse expression is covered; the
                    # transport/retry paths above stay outside this wrap.
                    raise LLMError(
                        f"malformed chat completions response: {response.text[:200]}",
                        response.status_code,
                    ) from error
                # A 200 is not a complete answer: a server that stops at its
                # output cap (finish_reason "length") or gets cut off by safety
                # filtering answers 200 with a truncated page. Handing that back
                # would commit half a page as a finished OCR.
                if finish_reason in ("length", "content_filter"):
                    raise LLMError(
                        f"chat completion ended early: finish_reason={finish_reason} "
                        f"after {len(content) if isinstance(content, str) else 0} characters",
                        None,
                        permanent=True,
                    )
                if not isinstance(content, str):
                    # nothing to hand back and nothing truncated to blame: this
                    # is the malformed-body error it always was (a non-string
                    # content, e.g. a parts list, lands here too rather than
                    # escaping as an AttributeError from .strip())
                    raise LLMError(
                        f"malformed chat completions response: {response.text[:200]}",
                        response.status_code,
                    )
                content = content.strip()
                if finish_reason is not None and finish_reason != "stop":
                    # Unknown reasons (servers invent their own) stay usable but
                    # must not pass silently.
                    logger.warning(
                        "unexpected finish_reason %r in chat completion (%d characters)",
                        finish_reason, len(content),
                    )
                # "stop", null or an absent field (plenty of servers omit it,
                # our own fake among them) is a normal completion.
                return content
            raise LLMError(
                f"chat completions failed: {response.status_code} {response.text[:200]}",
                response.status_code,
            )
        raise LLMError(f"chat completions failed after {MAX_ATTEMPTS} attempts: {last_error}")
