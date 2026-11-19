import ipaddress
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:  # annotation only: allowlist imports ConfigError from here
    from papra_suite.allowlist import AllowList


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
    #: The ceiling on one document's bytes, `PAPRA_MAX_DOCUMENT_BYTES`, default 100 MiB. One
    #: number for both of its consumers — see `OCR_MAX_DOCUMENT_BYTES` for why they are not two.
    max_document_bytes: int
    allowed_senders: "AllowList"
    # The index bridge. Both delivery flags default False: the bridge lands write-nothing,
    # and content/metadata write-back are switched on deliberately, per flag.
    kreuzakt_base_url: str
    index_content_delivery: bool
    index_metadata_delivery: bool
    legacy_workers: bool
    index_max_attempts: int
    index_search_scan_rows: int
    index_poll_interval_s: float
    index_wait_timeout_s: int
    index_query_max_chars: int
    mcp_url: str
    mcp_host: str
    mcp_port: int


DEFAULT_BIND = "127.0.0.1:8080"

#: Upper bound on one document's bytes, and the default behind `PAPRA_MAX_DOCUMENT_BYTES`.
#: Historical home: `papra_suite.pipeline`, which kept the name as a module attribute after the
#: knob landed (it is still what the OCR worker's own memory guard reads), and this is where the
#: number is now defined because `load_config` needs it and `pipeline` imports `config`, not the
#: other way round.
#:
#: ONE number for two consumers, deliberately. It bounds (a) the bytes the OCR worker will pull
#: into memory to render and (b) whether a document is enrolled in the ledger at all. They cannot
#: sensibly disagree: a document over the limit is never enrolled, so it is never indexed either,
#: and an index ceiling higher than the OCR ceiling would promise a population this suite has no
#: way to read. The operator's remedy for a document over the ceiling is the one the README names:
#: compress it and re-upload to Papra, after which the next pass enrols it.
OCR_MAX_DOCUMENT_BYTES = 100 * 1024 * 1024


def _safe_echo(value: str) -> str:
    """Echo only scheme://host — never userinfo, port text, path, or query."""
    # __main__ prints ConfigError messages to stderr unredacted, and a raw
    # value can hide a password in the userinfo, the port text, the path or
    # the query. Regex scrubbing is bypassable (scheme-less values never
    # contain "//", single-slash/backslash forms, secrets in the path,
    # passwords containing "@"), so echo ONLY what urlparse can structurally
    # identify: the scheme (if any) and the cleaned hostname. The port is
    # deliberately NOT accessed — a bad port raises ValueError, and the echo
    # sites are exactly the bad-port error paths; the host alone is enough
    # diagnosability.
    probe = value if "//" in value else f"//{value}"
    try:
        parsed = urlparse(probe)
        host = parsed.hostname  # None or cleaned host
    except ValueError:
        return "<unparseable value>"
    if not host:
        return "<unparseable value>"
    scheme = parsed.scheme
    return f"{scheme}://{host}" if scheme else f"//{host}"


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
            raise ConfigError(
                f"{name} must be an http(s) URL, got: {_safe_echo(value)!r}"
            )
        # A netloc like ":1221" passes the netloc check but has no host; httpx
        # would fail on every request. Likewise a bad port ("papra:abc",
        # "papra:99999", "host:1221:extra") only blows up inside httpx at
        # request time — the check command must reject it up front instead.
        if not parsed.hostname:
            raise ConfigError(
                f"{name} must include a host, got: {_safe_echo(value)!r}"
            )
        # httpx derives Basic auth credentials from URL userinfo, OVERRIDING
        # the Bearer API key header on every request (guaranteed 401s), and
        # it logs the credentialed URL at INFO — leaking the password to
        # stdout logs the redaction filter cannot catch. Reject up front.
        if parsed.username is not None or parsed.password is not None:
            # Do NOT echo the value here: it embeds the password, and
            # __main__ prints ConfigError messages to stderr unredacted.
            raise ConfigError(f"{name} must not contain credentials")
        # httpx concatenates a base URL's path with every request path
        # ("http://papra:1221/api" + "/api/api-keys/current" ->
        # "/api/api-keys/current" 404s on every call), so no input can make a
        # path-bearing base work with this client. After the rstrip a trailing
        # "/" is already gone, so any remaining path is a real path.
        if parsed.path not in ("",):
            raise ConfigError(
                f"{name} must not include a path, got: {_safe_echo(value)!r}"
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
            # Do NOT echo the value here: query strings carry secrets
            # (?token=...) that stderr printing would leak.
            raise ConfigError(f"{name} must not contain a query string")
        if _has_nondisplay_chars(parsed.hostname) or _has_nondisplay_chars(value):
            raise ConfigError(
                f"{name} must not contain whitespace or control characters,"
                f" got: {_safe_echo(value)!r}"
            )
        parsed.port  # noqa: B018 — property access validates the port
        # "http://papra:0": urlparse returns port 0 without raising, but httpx
        # keeps it and every request fails with ConnectError — same class as
        # the papra:99999/papra:abc cases. Port None is fine (scheme default).
        if parsed.port == 0:
            raise ConfigError(
                f"{name} must not use port 0, got: {_safe_echo(value)!r}"
            )
    except ValueError:
        # Covers non-numeric, out-of-range (0-65535) and trailing-garbage
        # ports, plus malformed IPv6 literals; urlparse raises on .port
        # access, not on parse.
        raise ConfigError(
            f"{name} must have a valid port and IPv6 literal,"
            f" got: {_safe_echo(value)!r}"
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


def _positive_int_env(env: Mapping[str, str], name: str, default: int) -> int:
    """Counters and budgets: a zero is not 'unlimited' and not 'off', it is a loop that
    does nothing or a scan that reads nothing, so it is refused rather than obeyed."""
    value = _int_env(env, name, default)
    if value < 1:
        raise ConfigError(f"{name} must be at least 1, got: {value}")
    return value


def _float_env(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        raise ConfigError(f"{name} must be a number, got: {raw!r}") from None


def _positive_float_env(env: Mapping[str, str], name: str, default: float) -> float:
    value = _float_env(env, name, default)
    if value <= 0:
        raise ConfigError(f"{name} must be greater than 0, got: {value}")
    return value


def _bool_env(env: Mapping[str, str], name: str, default: bool) -> bool:
    """Strict: a flag that silently falls back on a typo would turn a disabled delivery
    switch into an enabled one (or the reverse) with no message."""
    raw = env.get(name, "").strip().lower()
    if not raw:
        return default
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    raise ConfigError(f"{name} must be true or false, got: {raw!r}")


def _optional_base_url(env: Mapping[str, str], name: str, default: str) -> str:
    """`_require_base_url` for a value the operator may leave unset.

    The point of routing these two through the same validator is that a bad URL is a
    `papra-suite check` failure, not a transport error during the first upload: the
    userinfo, path, query and bad-port rejections in `_require_base_url` all describe
    values that would fail on every single request.
    """
    if not env.get(name, "").strip():
        return default
    return _require_base_url(env, name)


def _port_env(env: Mapping[str, str], name: str, default: int) -> int:
    value = _int_env(env, name, default)
    if not 1 <= value <= 65535:
        raise ConfigError(f"{name} must be a port between 1 and 65535, got: {value}")
    return value


def _host_env(env: Mapping[str, str], name: str, default: str) -> str:
    """A bare bind host — no scheme, no port, no userinfo.

    This value goes to a socket bind, so anything else is a paste error rather than a variant: a
    port belongs to `PAPRA_MCP_PORT` (a `host:port` here would bind the host and silently ignore the
    port), userinfo carries a credential that a bind cannot use and that stderr would echo, and a
    scheme means somebody wrote a URL into an interface field. The screening is the one
    `_parse_bind` already applies to `DASHBOARD_BIND`, minus the part about the port.

    The one spelling that is rewritten rather than refused is a bracketed IPv6 literal: `[::1]` is
    what a URL and `curl` use, `::1` is what `bind()` accepts, and they name the same interface.

    The refusal echoes through `_safe_echo` like every sibling here, because this is the one
    field an operator fills in by pasting from a browser bar, and that paste can carry
    `user:password@`. `__main__` prints `ConfigError` to stderr with no filter in front.
    """
    raw = env.get(name, "").strip()
    if not raw:
        return default
    if any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in raw):
        raise ConfigError(f"{name} must not contain whitespace or control characters")
    raw = _unwrap_ipv6_literal(raw)
    # One screen, not two: `_is_literal_bind` refuses a scheme, a path, userinfo, a bracketed
    # non-address and a `host:port` pair, because it accepts a colon only inside a real IPv6
    # literal. That is also why the bare `::1` gets through — it is a legal bind address, and
    # `LOOPBACK_HOSTS` and the Host allow-list both name it.
    if not _is_literal_bind(raw):
        raise ConfigError(f"{name} must be a bare host, got: {_safe_echo(raw)!r}")
    return raw.lower()


def _unwrap_ipv6_literal(raw: str) -> str:
    """`[::1]` and `::1` are the same literal; the socket wants the bare one.

    Unwraps **only** when the brackets hold a real IPv6 address. A bracketed hostname, or
    `[::1:8765`, or `[::1]:8765` with a port that belongs to `PAPRA_MCP_PORT`, is returned
    untouched so the check below can refuse it: silently repairing a paste error in a bind address
    is how a container ends up listening on something nobody named.
    """
    if raw.startswith("[") and raw.endswith("]"):
        inner = raw[1:-1]
        try:
            ipaddress.IPv6Address(inner)
        except ValueError:
            return raw
        return inner
    return raw


def _is_literal_bind(raw: str) -> bool:
    """An IP literal or a hostname — and nothing that could be read as something else.

    `127.0.0.1:8765` fails the single-colon rule in `_host_env`; `http://x` and `x/y` fail here;
    `::1` is the IPv6 loopback literal and is legal as a bind address, in bare form only — the
    bracketed spelling is turned into that form by `_unwrap_ipv6_literal` before this runs, so a
    bracket that survives to here belongs to something that is not an address.
    """
    if ":" in raw:
        try:
            ipaddress.IPv6Address(raw)
            return True
        except ValueError:
            return False
    if raw.startswith("[") or raw.endswith("]"):
        return False
    allowed = set("abcdefghijklmnopqrstuvwxyz0123456789.-_")
    return bool(raw) and set(raw.lower()) <= allowed and not raw.startswith("-")


def _parse_bind(raw: str) -> tuple[str, int]:
    try:
        parsed = urlparse(f"//{raw}")
        # A malformed IPv6 literal (e.g. "[::1:8080") makes urlparse itself
        # raise ValueError("Invalid IPv6 URL"), not just the .port access.
        port = parsed.port
    except ValueError:
        # Covers both a malformed IPv6 literal and an out-of-range port.
        raise ConfigError(
            f"DASHBOARD_BIND must be 'host:port' with a valid port,"
            f" got: {_safe_echo(raw)!r}"
        ) from None
    if not parsed.hostname or port is None:
        raise ConfigError(
            f"DASHBOARD_BIND must be 'host:port', got: {_safe_echo(raw)!r}"
        )
    # Userinfo in a listener bind is always a paste error (a bind address
    # carries no credentials); silently dropping it — as urlparse's
    # hostname accessor does — would bind a host the operator never named.
    # Do not echo the value: the userinfo may embed a password.
    if parsed.username is not None or parsed.password is not None:
        raise ConfigError("DASHBOARD_BIND must not contain credentials")
    if _has_nondisplay_chars(parsed.hostname) or _has_nondisplay_chars(raw):
        raise ConfigError(
            "DASHBOARD_BIND must not contain whitespace or control characters,"
            f" got: {_safe_echo(raw)!r}"
        )
    return parsed.hostname, port


def load_config(env: Mapping[str, str] | None = None) -> AppConfig:
    env = os.environ if env is None else env

    host, port = _parse_bind(env.get("DASHBOARD_BIND", "").strip() or DEFAULT_BIND)

    # Local import on purpose: allowlist raises ConfigError from this module, so
    # a module-level import here would be a cycle. Parsing at load time is the
    # point — a nonsense entry must stop `check` and `serve` at startup rather
    # than quietly vanish and leave the operator believing an entry that grants
    # nothing.
    from papra_suite.allowlist import parse_allowlist

    allowed_senders = parse_allowlist(env.get("PAPRA_ALLOWED_SENDERS", ""))

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
        # `_positive_int_env`, the port's and the counters' parser: `10MB`, `1e8` and `-5` refuse
        # with the bad value named rather than falling back to 100 MiB, which is the failure the
        # operator would never see — they raise the ceiling to admit a big scan, the typo is
        # swallowed, and the documents keep being refused at the default.
        max_document_bytes=_positive_int_env(env, "PAPRA_MAX_DOCUMENT_BYTES",
                                             OCR_MAX_DOCUMENT_BYTES),
        allowed_senders=allowed_senders,
        kreuzakt_base_url=_optional_base_url(env, "PAPRA_KREUZAKT_URL",
                                             "http://kreuzakt:3000"),
        index_content_delivery=_bool_env(env, "PAPRA_INDEX_CONTENT_DELIVERY", False),
        index_metadata_delivery=_bool_env(env, "PAPRA_INDEX_METADATA_DELIVERY", False),
        # True keeps today's behaviour (the ocr/title loops run and the legacy index job
        # is enqueued); the handover sets it false to stop those producers for good.
        legacy_workers=_bool_env(env, "PAPRA_LEGACY_WORKERS", True),
        index_max_attempts=_positive_int_env(env, "PAPRA_INDEX_MAX_ATTEMPTS", 3),
        index_search_scan_rows=_positive_int_env(env, "PAPRA_INDEX_SEARCH_SCAN_ROWS", 500),
        index_poll_interval_s=_positive_float_env(env, "PAPRA_INDEX_POLL_INTERVAL_S", 2.0),
        index_wait_timeout_s=_positive_int_env(env, "PAPRA_INDEX_WAIT_TIMEOUT_S", 900),
        index_query_max_chars=_positive_int_env(env, "PAPRA_INDEX_QUERY_MAX_CHARS", 512),
        # Empty is legal here and means "no MCP listener"; the MCP app is what refuses to
        # build without it. What is not legal is a URL that cannot be used as given.
        mcp_url=_optional_base_url(env, "PAPRA_MCP_URL", ""),
        # Loopback by default, and `resolve_bind` refuses anything else until a settings row agrees:
        # this listener is built to be published by `tailscale serve`, not by a reachable interface.
        mcp_host=_host_env(env, "PAPRA_MCP_HOST", "127.0.0.1"),
        mcp_port=_port_env(env, "PAPRA_MCP_PORT", 8765),
    )
