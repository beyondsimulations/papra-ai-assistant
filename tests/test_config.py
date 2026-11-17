from pathlib import Path

import pytest

from papra_suite.config import ConfigError, load_config

BASE_ENV = {
    "PAPRA_ALLOWED_SENDERS": "sender@x.com",
    "PAPRA_BASE_URL": "http://papra:1221",
    "PAPRA_API_KEY": "ppapi_" + "x" * 64,
    "LLM_BASE_URL": "http://llm:8000",
    "LLM_API_KEY": "sk-test",
    "TITLE_MODEL": "qwen3-4b",
    "OCR_MODEL": "qwen3-vl-8b",
    "ADMIN_PASSWORD": "correct-horse-battery-staple",
}


def test_load_config_applies_defaults(tmp_path):
    config = load_config({**BASE_ENV, "DATA_DIR": str(tmp_path)})

    assert config.dashboard_host == "127.0.0.1"
    assert config.dashboard_port == 8080
    assert config.ocr_max_pages == 50
    assert config.ocr_page_pixel_budget == 2_000_000
    assert config.ocr_dpi == 150
    assert config.ocr_concurrency == 3
    assert config.sweep_interval_s == 300
    assert config.papra_base_url == "http://papra:1221"


def test_load_config_strips_trailing_slashes():
    config = load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra:1221/"})

    assert config.papra_base_url == "http://papra:1221"


def test_slash_only_base_url_rejected():
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "/"})


def test_slash_only_llm_url_rejected():
    with pytest.raises(ConfigError, match="LLM_BASE_URL"):
        load_config({**BASE_ENV, "LLM_BASE_URL": "//"})


def test_missing_required_var_names_the_var():
    env = dict(BASE_ENV)
    del env["PAPRA_API_KEY"]

    with pytest.raises(ConfigError, match="PAPRA_API_KEY"):
        load_config(env)


def test_ocr_concurrency_hard_capped_at_3():
    config = load_config({**BASE_ENV, "OCR_CONCURRENCY": "10"})

    assert config.ocr_concurrency == 3


def test_dashboard_bind_parse():
    config = load_config({**BASE_ENV, "DASHBOARD_BIND": "0.0.0.0:9999"})

    assert config.dashboard_host == "0.0.0.0"
    assert config.dashboard_port == 9999


def test_bad_dashboard_bind_raises():
    with pytest.raises(ConfigError, match="DASHBOARD_BIND"):
        load_config({**BASE_ENV, "DASHBOARD_BIND": "nope"})


def test_invalid_integer_env_raises_config_error():
    with pytest.raises(ConfigError, match="OCR_MAX_PAGES"):
        load_config({**BASE_ENV, "OCR_MAX_PAGES": "fifty"})


def test_dashboard_bind_out_of_range_port_raises_config_error():
    with pytest.raises(ConfigError, match="DASHBOARD_BIND"):
        load_config({**BASE_ENV, "DASHBOARD_BIND": "127.0.0.1:99999"})


def test_malformed_ipv6_bind_raises_config_error():
    # A dropped closing bracket makes urlparse itself raise ValueError
    # ("Invalid IPv6 URL") before the .port guard; this must surface as a
    # ConfigError, not a traceback.
    with pytest.raises(ConfigError, match="DASHBOARD_BIND"):
        load_config({**BASE_ENV, "DASHBOARD_BIND": "[::1:8080"})


def test_scheme_only_base_url_rejected():
    # "http://" rstrips to "http:" — non-empty, but not a usable URL.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://"})


def test_schemeless_base_url_rejected():
    # "papra:1221" parses as scheme "papra" with empty netloc.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "papra:1221"})


def test_protocol_relative_base_url_rejected():
    # "//papra:1221" has no scheme at all.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "//papra:1221"})


def test_empty_host_base_url_rejected():
    # "http://:1221" has a netloc but no host; httpx would fail at request time.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://:1221"})


def test_non_numeric_port_base_url_rejected():
    # "http://papra:abc" — urlparse only raises on .port access.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra:abc"})


def test_port_zero_base_url_rejected():
    # "http://papra:0": urlparse returns port 0 without raising, but httpx
    # keeps it and every request fails with ConnectError — same class as the
    # papra:99999/papra:abc cases. Port 0 is only valid for a listener
    # (ephemeral port), never for a client base URL.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra:0"})


def test_normal_port_base_url_accepted():
    config = load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra:1221"})
    assert config.papra_base_url == "http://papra:1221"


def test_portless_base_url_accepted():
    # No port means the scheme default (80/443) — must still pass.
    config = load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra"})
    assert config.papra_base_url == "http://papra"


def test_dashboard_bind_port_zero_accepted():
    # Port 0 is the standard ephemeral-port convention for a listener and
    # must stay accepted in _parse_bind (only client base URLs reject it).
    config = load_config({**BASE_ENV, "DASHBOARD_BIND": "127.0.0.1:0"})
    assert config.dashboard_port == 0


def test_out_of_range_port_base_url_rejected():
    # "http://papra:99999" — out of the 0-65535 range.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra:99999"})


def test_trailing_garbage_after_port_base_url_rejected():
    # "http://host:1221:extra" — port accessor sees "1221:extra".
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://host:1221:extra"})


def test_base_url_with_user_and_password_rejected():
    # httpx would derive Basic auth from the userinfo, overriding the Bearer
    # API key header (every request 401s), and log the credentialed URL.
    with pytest.raises(ConfigError, match="credentials"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://user:pass@papra:1221"})


def test_base_url_with_username_only_rejected():
    # A bare username (no password) is equally fatal: httpx still builds
    # Basic auth credentials from it, overriding the Bearer header.
    with pytest.raises(ConfigError, match="credentials"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://user@papra:1221"})


def test_credentials_error_does_not_echo_value():
    # The credentials rejection must not echo the URL back: __main__ prints
    # ConfigError messages to stderr, an unredacted sink, and the URL embeds
    # the password. Same for the query rejection (?token=... carries secrets).
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL") as exc_info:
        load_config(
            {**BASE_ENV, "PAPRA_BASE_URL": "http://svc:secretpw@papra:1221"}
        )
    assert "secretpw" not in str(exc_info.value)
    assert "credentials" in str(exc_info.value)

    with pytest.raises(ConfigError, match="PAPRA_BASE_URL") as exc_info:
        load_config(
            {**BASE_ENV, "PAPRA_BASE_URL": "http://papra:1221?token=xyz"}
        )
    assert "xyz" not in str(exc_info.value)
    assert "query string" in str(exc_info.value)


def test_base_url_without_credentials_still_passes():
    config = load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra:1221"})
    assert config.papra_base_url == "http://papra:1221"


def test_valid_base_urls_still_pass():
    config = load_config({**BASE_ENV, "PAPRA_BASE_URL": "https://host.example"})
    assert config.papra_base_url == "https://host.example"


def test_malformed_ipv6_papra_base_url_raises_config_error():
    # "http://[::1" (unclosed bracket) makes urlparse itself raise
    # ValueError("Invalid IPv6 URL") at parse time — this must surface as a
    # ConfigError naming the variable, not a raw traceback.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://[::1"})


def test_malformed_ipv6_llm_base_url_raises_config_error():
    # "http://[invalid]:80" also fails at urlparse parse time.
    with pytest.raises(ConfigError, match="LLM_BASE_URL"):
        load_config({**BASE_ENV, "LLM_BASE_URL": "http://[invalid]:80"})


def test_valid_bracketed_ipv6_base_url_passes():
    config = load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://[::1]:1221"})
    assert config.papra_base_url == "http://[::1]:1221"


def test_base_url_with_space_in_host_rejected():
    # "http://my host:1221" parses fine, but httpx percent-encodes the space
    # and connects to a different host — every request would fail.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://my host:1221"})


def test_base_url_with_tab_rejected():
    # urlparse silently strips \t, but the httpx constructor raises
    # InvalidURL ("Invalid non-printable ASCII character in URL").
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": f"http://host{chr(9)}:1221"})


def test_dashboard_bind_with_space_in_host_rejected():
    # "12 7.0.0.1:8080" is not a bindable address; urlparse happily parses
    # it as hostname "12 7.0.0.1".
    with pytest.raises(ConfigError, match="DASHBOARD_BIND"):
        load_config({**BASE_ENV, "DASHBOARD_BIND": "12 7.0.0.1:8080"})


def test_whitespace_only_api_key_rejected():
    # _require must strip before the emptiness check, otherwise "   " is
    # blessed as a non-empty key and every request 401s.
    with pytest.raises(ConfigError, match="PAPRA_API_KEY"):
        load_config({**BASE_ENV, "PAPRA_API_KEY": "   "})


def test_required_values_are_stripped():
    config = load_config({**BASE_ENV, "PAPRA_API_KEY": "  ppapi_x  "})
    assert config.papra_api_key == "ppapi_x"


def test_control_chars_in_api_key_rejected():
    # "abc\rX-Injected: 1" passes the strip but h11 rejects the control char
    # at send time (LocalProtocolError -> TransportError), turning a
    # deterministic config error into 3 retries + a misleading PapraError.
    with pytest.raises(ConfigError, match="PAPRA_API_KEY"):
        load_config({**BASE_ENV, "PAPRA_API_KEY": "abc\rX-Injected: 1"})


def test_nul_byte_in_api_key_rejected():
    with pytest.raises(ConfigError, match="PAPRA_API_KEY"):
        load_config({**BASE_ENV, "PAPRA_API_KEY": "abc" + chr(0) + "def"})


def test_internal_space_in_api_key_accepted():
    # h11 accepts spaces inside header values (mere 401s), so a key
    # with an internal space is legitimate and must not be rejected.
    config = load_config({**BASE_ENV, "PAPRA_API_KEY": "ppapi_key with spaces"})
    assert config.papra_api_key == "ppapi_key with spaces"


def test_internal_tab_in_api_key_rejected():
    # Decision: keep the safe-side predicate (ord(ch) < 32 rejects tabs) —
    # a tab inside a secret is essentially always a paste error.
    with pytest.raises(ConfigError, match="PAPRA_API_KEY"):
        load_config({**BASE_ENV, "PAPRA_API_KEY": "ppapi_key" + chr(9) + "with-tab"})


def test_vertical_tab_in_admin_password_rejected():
    with pytest.raises(ConfigError, match="ADMIN_PASSWORD"):
        load_config({**BASE_ENV, "ADMIN_PASSWORD": "pass" + chr(11) + "word"})


def test_query_bearing_base_url_rejected():
    # "http://papra:1221?token=abc" parses fine, but httpx merges the query
    # into every request path — every call would 404. Must be rejected.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra:1221?token=abc"})


def test_bare_question_mark_base_url_rejected():
    # "http://papra:1221?" has an empty query per urlparse, but httpx
    # build_request mangles it into garbage (path "/", query b"/api/...")
    # — the same 404-everything failure mode as a non-empty query.
    # Must be rejected.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra:1221?"})


def test_fragment_only_base_url_still_passes():
    # "http://papra:1221#frag" has a fragment but no query — harmless.
    config = load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra:1221#frag"})
    assert config.papra_base_url == "http://papra:1221#frag"


def test_whitespace_only_int_env_uses_default():
    # _int_env must strip before the emptiness check, otherwise "   " hits
    # int("   ") -> ValueError instead of falling back to the default.
    config = load_config({**BASE_ENV, "OCR_MAX_PAGES": "   "})
    assert config.ocr_max_pages == 50


def test_data_dir_default():
    # The documented default data dir when DATA_DIR is unset.
    config = load_config(BASE_ENV)
    assert config.data_dir == Path("/app/data")


def test_path_bearing_base_url_rejected():
    # "http://papra:1221/api" parses fine, but httpx concatenates the base
    # path with every request path ("/api/api-keys/current" ->
    # "/api/api-keys/current" under the base path -> 404 on every call). No
    # input makes a path-bearing base work with this client — reject it.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra:1221/api"})


def test_path_bearing_base_url_with_trailing_slash_rejected():
    # The rstrip removes the trailing "/", leaving a real path — same
    # concatenation failure as above.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra:1221/api/"})


def test_pathless_base_urls_still_pass():
    # Both the bare and the trailing-slash form (stripped) must keep working.
    config = load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra:1221"})
    assert config.papra_base_url == "http://papra:1221"
    config = load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra:1221/"})
    assert config.papra_base_url == "http://papra:1221"


def test_control_chars_in_host_rejected():
    # NUL/ESC/DEL in a host: Python's urlparse happily accepts them, but
    # httpx raises InvalidURL on every request — the check command must not
    # bless such a config. This also pins _has_nondisplay_chars against an
    # isspace()-only regression (control characters are not whitespace).
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": f"http://ho{chr(0)}st:1221"})
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": f"http://ho{chr(0x1b)}st:1221"})


def test_control_chars_in_bind_rejected():
    # Same for DASHBOARD_BIND: a DEL byte in the host must not pass.
    with pytest.raises(ConfigError, match="DASHBOARD_BIND"):
        load_config({**BASE_ENV, "DASHBOARD_BIND": f"127.0.0.1{chr(0x7f)}:8080"})


def test_scheme_error_does_not_echo_password():
    # The scheme check runs before the credentials check, so its echo must
    # be sanitized: the password must not reach stderr, but the scheme must
    # remain for diagnosability.
    with pytest.raises(ConfigError) as excinfo:
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "ftp://svc:secretpw@papra:1221"})

    assert "secretpw" not in str(excinfo.value)
    assert "ftp" in str(excinfo.value)


def test_host_error_does_not_echo_password():
    # "http://svc:secretpw@" has no host; the host-check echo must not leak
    # the password.
    with pytest.raises(ConfigError) as excinfo:
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://svc:secretpw@"})

    assert "secretpw" not in str(excinfo.value)


def test_ipv6_error_does_not_echo_password():
    # A malformed IPv6 literal makes urlparse itself raise; the ValueError
    # handler echoes the value and must not leak the password.
    with pytest.raises(ConfigError) as excinfo:
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://[svc:secretpw@papra:1221"})

    assert "secretpw" not in str(excinfo.value)


def test_bind_bad_port_error_does_not_echo_password():
    # Scheme-less bind values never contain "//", so the old regex scrubber
    # never fired on them — the password reached stderr verbatim. The
    # structural helper must keep the host for diagnosability but drop
    # everything else.
    with pytest.raises(ConfigError, match="DASHBOARD_BIND") as excinfo:
        load_config({**BASE_ENV, "DASHBOARD_BIND": "u:secretpw@host:abc"})

    assert "secretpw" not in str(excinfo.value)
    assert "host" in str(excinfo.value)


def test_bind_userinfo_rejected():
    # "u:p@host:8080" used to be silently ACCEPTED with the userinfo
    # dropped — nonsense for a listener bind. Userinfo in a bind is always
    # a paste error and must be rejected outright, without echoing it.
    with pytest.raises(ConfigError, match="DASHBOARD_BIND") as excinfo:
        load_config({**BASE_ENV, "DASHBOARD_BIND": "u:secretpw@host:8080"})

    assert "secretpw" not in str(excinfo.value)


def test_schemeless_base_url_error_does_not_echo_password():
    # "svc:secretpw@papra:1221" has no "//" for the old regex to anchor
    # on; the scheme-check echo must not leak the password.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL") as excinfo:
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "svc:secretpw@papra:1221"})

    assert "secretpw" not in str(excinfo.value)


def test_single_slash_base_url_error_does_not_echo_password():
    # "http:/..." (single slash) never matched the "//"-anchored regex.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL") as excinfo:
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http:/u:secretpw@papra:1221"})

    assert "secretpw" not in str(excinfo.value)


def test_backslash_base_url_error_does_not_echo_password():
    # "http:\\..." equally bypassed the "//"-anchored regex.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL") as excinfo:
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http:\\u:secretpw@papra:1221"})

    assert "secretpw" not in str(excinfo.value)


def test_secret_in_path_base_url_does_not_echo_password():
    # "http://u:secretpw/q@papra:1221": the secret sits in the netloc but
    # the "@" lands in the path, defeating the userinfo regex; the echo
    # must never carry path text.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL") as excinfo:
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://u:secretpw/q@papra:1221"})

    assert "secretpw" not in str(excinfo.value)


def test_multi_at_password_fully_scrubbed_from_echo():
    # "ftp://u:secretpw@x@papra:1221": the password itself contains "@",
    # so the old regex only stripped up to the first "@" and leaked "x@".
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL") as excinfo:
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "ftp://u:secretpw@x@papra:1221"})

    assert "secretpw" not in str(excinfo.value)
    assert "x@" not in str(excinfo.value)


def test_query_stripped_from_echo():
    # The query check itself does not echo, but earlier checks (e.g. scheme)
    # might; the sanitizing helper must also strip query strings, which can
    # carry tokens.
    with pytest.raises(ConfigError) as excinfo:
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "ftp://papra:1221?tok=xyz"})

    assert "xyz" not in str(excinfo.value)


# --- the index bridge's own settings (plan Task 5 Step 4) --------------------
#
# Everything here is parsed at load time on purpose: a typo in PAPRA_KREUZAKT_URL or a
# zero poll interval must stop `papra-suite check`, not surface as a transport error
# during the first upload weeks later.


def test_index_defaults_are_write_nothing():
    config = load_config(BASE_ENV)

    assert config.kreuzakt_base_url == "http://kreuzakt:3000"
    assert config.index_content_delivery is False
    assert config.index_metadata_delivery is False
    assert config.index_max_attempts == 3
    assert config.index_search_scan_rows == 500
    assert config.index_poll_interval_s == 2.0
    assert config.index_wait_timeout_s == 900
    assert config.index_query_max_chars == 512
    assert config.mcp_url == ""
    assert config.mcp_port == 8765
    assert config.legacy_workers is True


def test_kreuzakt_url_is_read_and_normalised():
    config = load_config({**BASE_ENV, "PAPRA_KREUZAKT_URL": "http://kz:3000/"})

    assert config.kreuzakt_base_url == "http://kz:3000"


def test_kreuzakt_url_with_credentials_fails_at_check_time():
    # Same rule as PAPRA_BASE_URL: httpx derives Basic auth from URL userinfo and
    # logs the credentialed URL, so a value carrying a password must be rejected by
    # name and must not be echoed.
    with pytest.raises(ConfigError, match="PAPRA_KREUZAKT_URL") as excinfo:
        load_config({**BASE_ENV, "PAPRA_KREUZAKT_URL": "http://u:secretpw@kz:3000"})

    assert "secretpw" not in str(excinfo.value)


def test_kreuzakt_url_with_a_path_fails_at_check_time():
    with pytest.raises(ConfigError, match="PAPRA_KREUZAKT_URL"):
        load_config({**BASE_ENV, "PAPRA_KREUZAKT_URL": "http://kz:3000/api"})


def test_kreuzakt_url_must_be_a_url():
    with pytest.raises(ConfigError, match="PAPRA_KREUZAKT_URL"):
        load_config({**BASE_ENV, "PAPRA_KREUZAKT_URL": "kz:3000"})


def test_mcp_url_is_validated_by_the_same_rule_but_may_be_absent():
    config = load_config({**BASE_ENV, "PAPRA_MCP_URL": "https://papra-mcp.ts.net/"})
    assert config.mcp_url == "https://papra-mcp.ts.net"

    with pytest.raises(ConfigError, match="PAPRA_MCP_URL"):
        load_config({**BASE_ENV, "PAPRA_MCP_URL": "papra-mcp.ts.net"})

    with pytest.raises(ConfigError, match="PAPRA_MCP_URL") as excinfo:
        load_config({**BASE_ENV, "PAPRA_MCP_URL": "https://u:secretpw@papra-mcp.ts.net"})
    assert "secretpw" not in str(excinfo.value)


def test_mcp_port_must_be_a_port():
    config = load_config({**BASE_ENV, "PAPRA_MCP_PORT": "9999"})
    assert config.mcp_port == 9999

    with pytest.raises(ConfigError, match="PAPRA_MCP_PORT"):
        load_config({**BASE_ENV, "PAPRA_MCP_PORT": "0"})

    with pytest.raises(ConfigError, match="PAPRA_MCP_PORT"):
        load_config({**BASE_ENV, "PAPRA_MCP_PORT": "nope"})


def test_index_delivery_flags_parse_true():
    config = load_config({**BASE_ENV, "PAPRA_INDEX_CONTENT_DELIVERY": "true",
                          "PAPRA_INDEX_METADATA_DELIVERY": "1"})

    assert config.index_content_delivery is True
    assert config.index_metadata_delivery is True


def test_index_delivery_flag_with_a_non_boolean_value_is_rejected():
    with pytest.raises(ConfigError, match="PAPRA_INDEX_CONTENT_DELIVERY"):
        load_config({**BASE_ENV, "PAPRA_INDEX_CONTENT_DELIVERY": "maybe"})


def test_legacy_workers_can_be_turned_off_for_the_handover():
    config = load_config({**BASE_ENV, "PAPRA_LEGACY_WORKERS": "false"})

    assert config.legacy_workers is False


def test_index_poll_interval_must_be_positive():
    # A zero poll interval is a hot loop against Kreuzakt; it must not load.
    with pytest.raises(ConfigError, match="PAPRA_INDEX_POLL_INTERVAL_S"):
        load_config({**BASE_ENV, "PAPRA_INDEX_POLL_INTERVAL_S": "0"})

    with pytest.raises(ConfigError, match="PAPRA_INDEX_POLL_INTERVAL_S"):
        load_config({**BASE_ENV, "PAPRA_INDEX_POLL_INTERVAL_S": "-1"})

    config = load_config({**BASE_ENV, "PAPRA_INDEX_POLL_INTERVAL_S": "0.25"})
    assert config.index_poll_interval_s == 0.25


def test_index_poll_interval_with_text_is_rejected_by_name():
    with pytest.raises(ConfigError, match="PAPRA_INDEX_POLL_INTERVAL_S"):
        load_config({**BASE_ENV, "PAPRA_INDEX_POLL_INTERVAL_S": "two"})


def test_index_counters_must_be_positive():
    for name in ("PAPRA_INDEX_MAX_ATTEMPTS", "PAPRA_INDEX_SEARCH_SCAN_ROWS",
                 "PAPRA_INDEX_WAIT_TIMEOUT_S", "PAPRA_INDEX_QUERY_MAX_CHARS"):
        with pytest.raises(ConfigError, match=name):
            load_config({**BASE_ENV, name: "0"})

    for name in ("PAPRA_INDEX_MAX_ATTEMPTS", "PAPRA_INDEX_SEARCH_SCAN_ROWS",
                 "PAPRA_INDEX_WAIT_TIMEOUT_S", "PAPRA_INDEX_QUERY_MAX_CHARS"):
        with pytest.raises(ConfigError, match=name):
            load_config({**BASE_ENV, name: "-3"})


def test_index_integer_env_with_text_is_rejected_by_name():
    with pytest.raises(ConfigError, match="PAPRA_INDEX_MAX_ATTEMPTS"):
        load_config({**BASE_ENV, "PAPRA_INDEX_MAX_ATTEMPTS": "three"})


def test_index_whitespace_only_env_falls_back_to_the_default():
    config = load_config({**BASE_ENV, "PAPRA_INDEX_WAIT_TIMEOUT_S": "  ",
                          "PAPRA_KREUZAKT_URL": ""})

    assert config.index_wait_timeout_s == 900
    assert config.kreuzakt_base_url == "http://kreuzakt:3000"
