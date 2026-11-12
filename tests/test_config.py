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
