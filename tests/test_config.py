from pathlib import Path

import pytest

from papra_suite.config import ConfigError, load_config

BASE_ENV = {
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


def test_out_of_range_port_base_url_rejected():
    # "http://papra:99999" — out of the 0-65535 range.
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://papra:99999"})


def test_trailing_garbage_after_port_base_url_rejected():
    # "http://host:1221:extra" — port accessor sees "1221:extra".
    with pytest.raises(ConfigError, match="PAPRA_BASE_URL"):
        load_config({**BASE_ENV, "PAPRA_BASE_URL": "http://host:1221:extra"})


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
