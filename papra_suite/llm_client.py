"""Thin async client for OpenAI-compatible chat-completions endpoints.

Mirrors papra_client's retry policy: 3 attempts, exponential backoff,
retry on 429 + all 5xx + network errors; other 4xx fail permanently.
"""

import asyncio
import base64

import httpx

MAX_ATTEMPTS = 3


class LLMError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


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
                response = await self._client.post("/chat/completions", json=body)
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
                    return response.json()["choices"][0]["message"]["content"].strip()
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
            raise LLMError(
                f"chat completions failed: {response.status_code} {response.text[:200]}",
                response.status_code,
            )
        raise LLMError(f"chat completions failed after {MAX_ATTEMPTS} attempts: {last_error}")
