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


def _has_nondisplay_chars(text: str) -> bool:
    # httpx percent-encodes spaces ("http://my host" -> a different host) and
    # raises InvalidURL on tabs/control chars, but Python's urlparse silently
    # strips \t/\r/\n before parsing — so both the parsed hostname and the
    # raw value must be screened.
    return any(ch.isspace() or not ch.isprintable() for ch in text)


def _has_control_chars(text: str) -> bool:
    # Spaces are legitimate inside header values (h11 accepts them, e.g. an
    # API key with an internal space just yields 401s), but tabs are rejected
    # as likely paste errors, and control characters are not: h11 rejects them
    # at send time (LocalProtocolError), turning a deterministic config error
    # into a misleading TransportError after retries. Screen ord(ch) < 32
    # (which includes tabs) plus DEL.
    return any(ord(ch) < 32 or ord(ch) == 127 for ch in text)


def _require(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    if _has_control_chars(value):
        # Do not echo the value: it may be a secret.
        raise ConfigError(f"{name} must not contain control characters")
    return value


def _require_base_url(env: Mapping[str, str], name: str) -> str:
    # Validate the post-rstrip result: a slash-only value like "/" passes the
    # non-empty check but strips to "", which would make every client call fail
    # at runtime. Likewise scheme-less ("papra:1221") or scheme-only
    # ("http://" -> "http:") values must not be blessed. The check command
    # must not bless such a config.
    value = _require(env, name).rstrip("/")
    if not value:
        raise ConfigError(f"{name} must not be empty")
    try:
        # A malformed IPv6 literal (e.g. "http://[::1") makes urlparse itself
        # raise ValueError("Invalid IPv6 URL") at parse time, not only on the
        # .port access — mirror the DASHBOARD_BIND handling in _parse_bind.
        parsed = urlparse(value)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ConfigError(f"{name} must be an http(s) URL, got: {value!r}")
        # A netloc like ":1221" passes the netloc check but has no host; httpx
        # would fail on every request. Likewise a bad port ("papra:abc",
        # "papra:99999", "host:1221:extra") only blows up inside httpx at
        # request time — the check command must reject it up front instead.
        if not parsed.hostname:
            raise ConfigError(f"{name} must include a host, got: {value!r}")
        # httpx derives Basic auth credentials from URL userinfo, OVERRIDING
        # the Bearer API key header on every request (guaranteed 401s), and
        # it logs the credentialed URL at INFO — leaking the password to
        # stdout logs the redaction filter cannot catch. Reject up front.
        if parsed.username is not None or parsed.password is not None:
            raise ConfigError(f"{name} must not contain credentials, got: {value!r}")
        # httpx concatenates a base URL's path with every request path
        # ("http://papra:1221/api" + "/api/api-keys/current" ->
        # "/api/api-keys/current" 404s on every call), so no input can make a
        # path-bearing base work with this client. After the rstrip a trailing
        # "/" is already gone, so any remaining path is a real path.
        if parsed.path not in ("",):
            raise ConfigError(
                f"{name} must not include a path, got: {value!r}"
            )
        # httpx merges a base URL's query string into every request path
        # (e.g. "http://papra:1221?token=abc" + "/api/..." -> garbage), so
        # every call would 404. A bare trailing "?" ("http://papra:1221?")
        # is equally fatal — urlparse reports an empty query, but httpx
        # build_request turns it into garbage (path "/", query b"/api/...")
        # — so reject any "?" before the fragment, empty or not. A fragment
        # ("...#frag") without a query is harmless.
        before_fragment = value.split("#", 1)[0]
        if "?" in before_fragment:
            raise ConfigError(
                f"{name} must not contain a query string, got: {value!r}"
            )
        if _has_nondisplay_chars(parsed.hostname) or _has_nondisplay_chars(value):
            raise ConfigError(
                f"{name} must not contain whitespace or control characters, got: {value!r}"
            )
        parsed.port  # noqa: B018 — property access validates the port
        # "http://papra:0": urlparse returns port 0 without raising, but httpx
        # keeps it and every request fails with ConnectError — same class as
        # the papra:99999/papra:abc cases. Port None is fine (scheme default).
        if parsed.port == 0:
            raise ConfigError(f"{name} must not use port 0, got: {value!r}")
    except ValueError:
        # Covers non-numeric, out-of-range (0-65535) and trailing-garbage
        # ports, plus malformed IPv6 literals; urlparse raises on .port
        # access, not on parse.
        raise ConfigError(
            f"{name} must have a valid port and IPv6 literal, got: {value!r}"
        ) from None
    # "http://papra:" (empty port) is indistinguishable from "http://papra"
    # via parsed attributes (netloc "papra:" but hostname/port identical to
    # the no-port form), and httpx normalizes both to the scheme default
    # (port None -> 80/443). Verified against httpx.URL: both produce
    # host "papra", port None. So both are accepted here.
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
    try:
        parsed = urlparse(f"//{raw}")
        # A malformed IPv6 literal (e.g. "[::1:8080") makes urlparse itself
        # raise ValueError("Invalid IPv6 URL"), not just the .port access.
        port = parsed.port
    except ValueError:
        # Covers both a malformed IPv6 literal and an out-of-range port.
        raise ConfigError(
            f"DASHBOARD_BIND must be 'host:port' with a valid port, got: {raw!r}"
        ) from None
    if not parsed.hostname or port is None:
        raise ConfigError(f"DASHBOARD_BIND must be 'host:port', got: {raw!r}")
    if _has_nondisplay_chars(parsed.hostname) or _has_nondisplay_chars(raw):
        raise ConfigError(
            f"DASHBOARD_BIND must not contain whitespace or control characters, got: {raw!r}"
        )
    return parsed.hostname, port


def load_config(env: Mapping[str, str] | None = None) -> AppConfig:
    env = os.environ if env is None else env

    host, port = _parse_bind(env.get("DASHBOARD_BIND", "").strip() or DEFAULT_BIND)

    return AppConfig(
        papra_base_url=_require_base_url(env, "PAPRA_BASE_URL"),
        papra_api_key=_require(env, "PAPRA_API_KEY"),
        llm_base_url=_require_base_url(env, "LLM_BASE_URL"),
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
