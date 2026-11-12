import json
import logging
import sys
from datetime import UTC, datetime

REDACTED = "[REDACTED]"


class SecretRegistry:
    """Single shared registry of secret values; one filter consults it."""

    def __init__(self) -> None:
        self._secrets: set[str] = set()

    def add(self, value: str | None) -> None:
        if value:
            self._secrets.add(value)

    def redact(self, text: str) -> str:
        for secret in self._secrets:
            if secret in text:
                text = text.replace(secret, REDACTED)
        return text


SECRET_REGISTRY = SecretRegistry()


class RedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        record.msg = SECRET_REGISTRY.redact(message)
        record.args = None
        if record.exc_info:
            record.exc_text = SECRET_REGISTRY.redact(
                logging.Formatter().formatException(record.exc_info)
            )
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = record.exc_text or self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class RingBufferHandler(logging.Handler):
    def __init__(self, store) -> None:
        super().__init__()
        self._store = store

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._store.append_log_line(
                ts=datetime.now(UTC).isoformat(),
                level=record.levelname,
                line=self.format(record),
            )
        except Exception:  # noqa: BLE001 - logging must never crash the app
            self.handleError(record)


def setup_logging(store=None) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(JsonFormatter())
    stream.addFilter(RedactingFilter())
    root.addHandler(stream)

    if store is not None:
        ring = RingBufferHandler(store)
        ring.setFormatter(logging.Formatter("%(name)s: %(message)s"))
        ring.addFilter(RedactingFilter())
        root.addHandler(ring)
