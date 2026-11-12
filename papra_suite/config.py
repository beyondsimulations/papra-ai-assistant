import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class AppConfig:
    papra_base_url: str
    papra_api_key: str
    llm_base_url: str
    llm_api_key: str
    title_model: str
    ocr_model: str
    admin_password: str
    dashboard_host: str
    dashboard_port: int
    data_dir: Path
    ocr_max_pages: int
    ocr_page_pixel_budget: int
    ocr_dpi: int
    ocr_concurrency: int
    sweep_interval_s: int


DEFAULT_BIND = "127.0.0.1:8080"


def _require(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def _int_env(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ConfigError(f"{name} must be an integer, got: {raw!r}") from None


def _parse_bind(raw: str) -> tuple[str, int]:
    parsed = urlparse(f"//{raw}")
    try:
        port = parsed.port
    except ValueError:
        raise ConfigError(
            f"DASHBOARD_BIND port must be between 0 and 65535, got: {raw!r}"
        ) from None
    if not parsed.hostname or port is None:
        raise ConfigError(f"DASHBOARD_BIND must be 'host:port', got: {raw!r}")
    return parsed.hostname, port


def load_config(env: Mapping[str, str] | None = None) -> AppConfig:
    env = os.environ if env is None else env

    host, port = _parse_bind(env.get("DASHBOARD_BIND", "").strip() or DEFAULT_BIND)

    return AppConfig(
        papra_base_url=_require(env, "PAPRA_BASE_URL").rstrip("/"),
        papra_api_key=_require(env, "PAPRA_API_KEY"),
        llm_base_url=_require(env, "LLM_BASE_URL").rstrip("/"),
        llm_api_key=_require(env, "LLM_API_KEY"),
        title_model=_require(env, "TITLE_MODEL"),
        ocr_model=_require(env, "OCR_MODEL"),
        admin_password=_require(env, "ADMIN_PASSWORD"),
        dashboard_host=host,
        dashboard_port=port,
        data_dir=Path(env.get("DATA_DIR", "").strip() or "/app/data"),
        ocr_max_pages=_int_env(env, "OCR_MAX_PAGES", 50),
        ocr_page_pixel_budget=_int_env(env, "OCR_PAGE_PIXEL_BUDGET", 2_000_000),
        ocr_dpi=_int_env(env, "OCR_DPI", 150),
        ocr_concurrency=min(_int_env(env, "OCR_CONCURRENCY", 3), 3),
        sweep_interval_s=_int_env(env, "SWEEP_INTERVAL_S", 300),
    )
