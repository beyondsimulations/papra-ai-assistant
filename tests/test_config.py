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
