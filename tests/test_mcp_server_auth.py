"""Task 12, part 2: the MCP transport's front door — Host, Origin, bearer token, and the 401s.

Everything here is about requests that must not reach a tool. The 2026-11-16 spec measured this
surface on `mcp==2.3.0` and its numbers are the contract, so the tests assert the SDK's own status
codes rather than our opinions: 401 for missing, wrong and query-string credentials; 421 for a Host
the allow-list does not name; 403 for an Origin it does not name; and a successful initialize that
carries a session id.

Two things in this file are deliberately *not* the SDK's default behaviour, and both are the spec's
findings rather than inventions:

**Host/Origin is checked before authentication.** `streamable_http_app()` validates those headers
inside the session manager, i.e. behind `RequireAuthMiddleware`, so an unauthenticated request with
a spoofed Host answers 401 and the 421 never happens. That is not a leak — it is the diagnostic
disappearing: the deployment blocker in the spec is a proxy presenting a host nobody knew was being
rejected. So the SDK's own `TransportSecurityMiddleware` is run earlier by our wrapper, and
`test_bare_host_gets_421_without_a_credential` is what stops that position regressing.

**`resource_server_url=None`.** With it, the bearer gate is still enforced on every request, no
`resource_metadata` pointer is advertised, and no Protected Resource Metadata route is served. That
is the shipped configuration, and it is version-pinned behaviour, so it is asserted here rather
than trusted in prose.
"""

import json
import logging
import sys

import pytest
from starlette.testclient import TestClient

from papra_suite.mcp_server import (
    DEFAULT_MCP_HOST,
    MCP_ENABLED_SETTING,
    MCP_NONLOOPBACK_SETTING,
    McpConfigError,
    build_mcp_app,
    resolve_bind,
    transport_security_for,
)
from papra_suite.mcp_tokens import mint_token, revoke_token
from papra_suite.store import Store
from tests.test_config import BASE_ENV

PUBLIC_URL = "https://papra-mcp.ts.net"
HOST = "papra-mcp.ts.net"
ORG = "org-1"


class OkKreuzakt:
    """The two calls `index_status` makes, with the calls counted."""

    def __init__(self, *, counts=None):
        self.counts = {"pending": 3, "completed": 12} if counts is None else counts
        self.health_calls = 0
        self.queue_calls = 0

    async def health(self):
        self.health_calls += 1
        return {"status": "ok", "documents": 12}

    async def queue_counts(self):
        self.queue_calls += 1
        return dict(self.counts)


class OkPapra:
    def __init__(self):
        self.calls = 0

    async def list_organizations(self):
        self.calls += 1
        return []


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state.db")
    store.migrate()
    yield store
    store.close()


@pytest.fixture
def token(store):
    return mint_token(store, label="pi", org_ids=[ORG])


def app_for(store, url=PUBLIC_URL, kz=None, papra=None):
    return build_mcp_app(store, papra or OkPapra(), kz or OkKreuzakt(), url=url)


def client_for(app):
    """A TestClient with the lifespan running: the session manager needs it to start."""
    return TestClient(app)


def envelope(method, params, request_id=1):
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}


def initialize_headers(token=None, host=HOST):
    headers = {"host": host, "content-type": "application/json",
               "accept": "application/json, text/event-stream"}
    if token:
        headers["authorization"] = f"Bearer {token}"
    return headers


def initialize(client, token, host=HOST):
    response = client.post("/mcp", headers=initialize_headers(token, host),
                           json=envelope("initialize", {
                               "protocolVersion": "2025-11-25", "capabilities": {},
                               "clientInfo": {"name": "pytest", "version": "0"}}))
    return response, response.headers.get("mcp-session-id")


# --- 401: the bearer gate ---------------------------------------------------------------

def test_wrong_and_missing_token_return_the_same_401(store):
    with client_for(app_for(store)) as client:
        no_key = client.get("/mcp", headers=initialize_headers())
        bad_key = client.get("/mcp", headers=initialize_headers("not-a-token"))

    assert no_key.status_code == bad_key.status_code == 401
    assert no_key.text == bad_key.text            # no oracle for "token exists"


def test_a_token_with_one_character_appended_is_a_missing_token(store, token):
    """No prefix matching, on the wire as well as in the store.

    The 401 body must also be the *same* one the absent credential produces, or the difference
    between "no such token" and "such a token, badly typed" becomes a probe for valid prefixes.
    """
    with client_for(app_for(store)) as client:
        absent = client.get("/mcp", headers=initialize_headers())
        extended = client.get("/mcp", headers=initialize_headers(token + "x"))
        truncated = client.get("/mcp", headers=initialize_headers(token[:-1]))

    assert extended.status_code == truncated.status_code == absent.status_code
    assert extended.text == truncated.text == absent.text


def test_query_string_token_is_rejected(store, token):
    with client_for(app_for(store)) as client:
        response = client.get("/mcp?token=" + token, headers=initialize_headers())
        access_token = client.get("/mcp?access_token=" + token, headers=initialize_headers())

    assert response.status_code == 401       # tokens never travel in URLs
    assert access_token.status_code == 401


def test_an_existing_session_id_is_not_authentication(store, token):
    """A session id is a handle to a session somebody else authenticated into, or to none at all."""
    with client_for(app_for(store)) as client:
        _, session = initialize(client, token)
        headers = {"host": HOST}
        if session:
            headers["mcp-session-id"] = session
        response = client.get("/mcp", headers=headers)
        post = client.post("/mcp", headers=dict(headers, **{"content-type": "application/json"}),
                           json=envelope("tools/list", {}))

    assert response.status_code == 401
    assert post.status_code == 401


def test_an_empty_token_store_authenticates_nothing(store):
    guess = "x" * 43
    with client_for(app_for(store)) as client:
        response = client.post("/mcp", headers=initialize_headers(guess),
                               json=envelope("initialize", {
                                   "protocolVersion": "2025-11-25", "capabilities": {},
                                   "clientInfo": {"name": "pytest", "version": "0"}}))
    assert response.status_code == 401


def test_the_401_does_not_echo_what_was_presented(store):
    secret = "unrepeatable-string-not-a-real-token"
    with client_for(app_for(store)) as client:
        response = client.get("/mcp", headers=initialize_headers(secret))
    assert secret not in response.text
    assert secret not in str(response.headers)


def test_a_revoked_token_stops_working_on_the_next_request(store):
    """Revocation needs no restart, because the scope is read from the store per request.

    The first half of this test is the session being established; the second is the same client,
    same headers, one request later, refused. A server that cached the principal per session would
    pass every other test in this file and fail this one.
    """
    token = mint_token(store, label="hermes", org_ids=[ORG])
    with client_for(app_for(store)) as client:
        first, session = initialize(client, token)
        assert first.status_code == 200 and session
        revoke_token(store, label="hermes")
        headers = initialize_headers(token)
        headers["mcp-session-id"] = session
        after = client.post("/mcp", headers=headers, json=envelope("tools/list", {}, 2))

    assert after.status_code == 401


# --- 421 / 403: the Host and Origin allow-list -------------------------------------------

def test_bare_host_gets_421(store):
    """The 11-16 spec's deployment blocker, kept: a bare Host never reaches MCP code."""
    with client_for(app_for(store)) as client:
        response = client.post("/mcp", headers={"host": "100.64.1.20:8765",
                                                "content-type": "application/json"}, json={})
    assert response.status_code == 421


def test_bare_host_gets_421_without_a_credential(store, token):
    """The check runs before authentication, so the answer does not depend on the token.

    With the SDK's own ordering this request answers 401 — the operator with the misconfigured
    proxy then reads "my token is wrong" and tunes the wrong thing.
    """
    with client_for(app_for(store)) as client:
        response = client.post("/mcp", headers={"host": "100.64.1.20:8765",
                                                "content-type": "application/json",
                                                "authorization": f"Bearer {token}"}, json={})
    assert response.status_code == 421


def test_the_public_host_is_accepted(store, token):
    with client_for(app_for(store)) as client:
        response, session = initialize(client, token)
    assert response.status_code == 200 and session


def test_the_public_host_with_the_serve_port_is_accepted(store, token):
    """`tailscale serve` forwards the bare host; a client that spells the port must still work."""
    with client_for(app_for(store)) as client:
        response, _ = initialize(client, token, host=f"{HOST}:443")
    assert response.status_code == 200


def test_an_unlisted_origin_gets_403(store, token):
    with client_for(app_for(store)) as client:
        response = client.post("/mcp", headers=dict(
            initialize_headers(token), origin="http://evil.example.com"),
            json=envelope("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                                         "clientInfo": {"name": "pytest", "version": "0"}}))
    assert response.status_code == 403


def test_the_allowed_hosts_are_lowercase_and_hold_no_wildcard_port(store):
    """Both SDK bugs the spec measured are pinned by this shape.

    python-sdk#3437: an uppercase entry rejects every WHATWG-`fetch` client, because `fetch` sends
    the host lowercased. python-sdk#3463: `host:*` accepts `host:9000.evil`, because the "port"
    suffix is never validated. So: no `:*` entries at all, and nothing that changes when cased.
    """
    settings = transport_security_for(PUBLIC_URL, host=DEFAULT_MCP_HOST, port=8765)

    assert settings.allowed_hosts
    assert settings.enable_dns_rebinding_protection is True
    for entry in settings.allowed_hosts + settings.allowed_origins:
        assert not entry.endswith(":*"), entry
        assert entry == entry.lower(), entry
    assert HOST in settings.allowed_hosts
    assert f"{HOST}:443" in settings.allowed_hosts


def test_the_loopback_listener_stays_reachable_for_a_local_health_check(store):
    settings = transport_security_for(PUBLIC_URL, host="127.0.0.1", port=8765)
    assert "127.0.0.1:8765" in settings.allowed_hosts
    assert "localhost:8765" in settings.allowed_hosts
    assert "http://127.0.0.1:8765" in settings.allowed_origins


# --- configuration refusals ---------------------------------------------------------------

def test_mcp_url_must_be_configured_or_startup_fails(store):
    with pytest.raises(ValueError, match="PAPRA_MCP_URL"):
        build_mcp_app(store, OkPapra(), OkKreuzakt(), url="")


def test_a_malformed_public_url_refuses_startup(store):
    for bad in ("not a url", "ftp://papra-mcp.ts.net", "https://"):
        with pytest.raises(ValueError, match="PAPRA_MCP_URL"):
            build_mcp_app(store, OkPapra(), OkKreuzakt(), url=bad)


def test_a_nonloopback_bind_is_refused_and_names_what_was_refused(tmp_path):
    from types import SimpleNamespace

    config = SimpleNamespace(mcp_host="0.0.0.0", mcp_port=8765, data_dir=tmp_path)
    with pytest.raises(McpConfigError, match="0.0.0.0"):
        resolve_bind(config)


def test_a_nonloopback_bind_needs_the_setting_and_then_is_allowed(tmp_path):
    from types import SimpleNamespace

    config = SimpleNamespace(mcp_host="100.64.1.20", mcp_port=8765, data_dir=tmp_path)
    store = Store(tmp_path / "state.db")
    store.migrate()
    try:
        with pytest.raises(McpConfigError, match=MCP_NONLOOPBACK_SETTING):
            resolve_bind(config, store=store)
        store.set_setting(MCP_NONLOOPBACK_SETTING, "1")
        bind = resolve_bind(config, store=store)
        assert (bind.host, bind.port) == ("100.64.1.20", 8765)
    finally:
        store.close()


@pytest.mark.parametrize("loopback", ["127.0.0.1", "localhost", "::1"])
def test_loopback_needs_no_permission(tmp_path, loopback):
    from types import SimpleNamespace

    config = SimpleNamespace(mcp_host=loopback, mcp_port=8765, data_dir=tmp_path)
    assert resolve_bind(config).host == loopback


def test_the_mcp_port_comes_from_the_environment():
    from papra_suite.config import load_config

    config = load_config(dict(BASE_ENV, PAPRA_MCP_HOST="127.0.0.1", PAPRA_MCP_PORT="8123",
                              PAPRA_MCP_URL=PUBLIC_URL))
    assert (config.mcp_host, config.mcp_port, config.mcp_url) == ("127.0.0.1", 8123, PUBLIC_URL)


def test_a_bad_mcp_host_is_a_configuration_error():
    from papra_suite.config import ConfigError, load_config

    for bad in ("http://127.0.0.1", "0.0.0.0:8765", "has space", "user@127.0.0.1"):
        with pytest.raises(ConfigError):
            load_config(dict(BASE_ENV, PAPRA_MCP_HOST=bad))


def test_an_unset_mcp_host_is_the_loopback_default():
    from papra_suite.config import load_config

    assert load_config(dict(BASE_ENV)).mcp_host == DEFAULT_MCP_HOST


# --- what the shipped configuration advertises -------------------------------------------

def test_the_bearer_gate_is_enforced_without_advertising_a_dead_issuer(store, token):
    """`resource_server_url=None`: gate on, fake issuer advertised nowhere (spec, "pick one").

    With a resource URL set, the SDK serves Protected Resource Metadata pointing at an
    `authorization_servers` issuer that 404s, and an OAuth-capable client follows that pointer into
    a dead end. Unset, the client gets a 404 for the metadata instead of a detour — recorded in the
    spec, and here, because an SDK upgrade must not be able to change it quietly.
    """
    with client_for(app_for(store)) as client:
        unauthenticated = client.get("/mcp", headers=initialize_headers())
        metadata = client.get("/.well-known/oauth-protected-resource", headers={"host": HOST})
        issuer = client.get("/.well-known/oauth-authorization-server", headers={"host": HOST})

    assert unauthenticated.status_code == 401
    assert "resource_metadata" not in unauthenticated.headers.get("www-authenticate", "")
    assert metadata.status_code == 404
    assert issuer.status_code == 404


def test_a_valid_token_creates_a_session_the_client_can_reuse(store, token):
    """The success path, asserted rather than assumed.

    The SDK's `AccessToken` requires `client_id` and `scopes`; a verifier that returns a bare token
    raises inside `verify_token`, so the *correct* credential answers 500 while every wrong one
    looks perfectly healthy. Only the happy path catches that.
    """
    with client_for(app_for(store)) as client:
        response, session = initialize(client, token)
        assert response.status_code == 200 and session
        headers = initialize_headers(token)
        headers["mcp-session-id"] = session
        listed = client.post("/mcp", headers=headers, json=envelope("tools/list", {}, 2))
        body = [json.loads(line[6:]) for line in listed.text.splitlines()
                if line.startswith("data: ")]
        names = [tool["name"] for tool in body[0]["result"]["tools"]]

        closed = client.delete("/mcp", headers=headers)

    assert "index_status" in names
    assert closed.status_code == 200


# --- logs and stdout ----------------------------------------------------------------------

def test_the_token_never_appears_in_the_server_logs_or_on_stdout(store, capsys, caplog):
    """What this asserts is about our process, not the test client's HTTP library.

    The spec measured uvicorn's access log writing full URLs **including query strings** to stdout,
    with a refused query-string token appearing verbatim in it. So: `access_log=False` (asserted as
    "no `uvicorn.access` record was emitted at all"), the SDK and suite loggers carry no
    plaintext, and nothing at all lands on stdout. The `httpx` logger is out of scope on purpose:
    in production this process makes no outbound request that carries an MCP token, and that logger
    belongs to whatever client is dialling us.
    """
    token = mint_token(store, label="logger-check", org_ids=[ORG])
    ours = ("mcp", "papra_suite", "uvicorn")
    with caplog.at_level(logging.DEBUG):
        with client_for(app_for(store)) as client:
            initialize(client, token)
            client.get("/mcp", headers=initialize_headers("nope"))
            client.get("/mcp?access_token=" + token, headers=initialize_headers())
    lines = [record.getMessage() for record in caplog.records
             if record.name.split(".")[0] in ours]
    assert lines, "the server logged nothing at all, so this test would prove nothing"
    assert not [line for line in lines if token in line]
    assert not [record for record in caplog.records if record.name.startswith("uvicorn.access")]
    captured = capsys.readouterr()
    assert token not in captured.out
    assert token not in captured.err


def test_a_logger_that_echoes_the_credential_is_redacted_and_not_printed(store, capsys):
    """The suite's secret registry covers the credential this task introduces.

    `RedactingFilter` scrubs registered secrets from every line the suite writes, and it is the only
    defence that works against a third-party logger: HTTP client libraries log full URLs at INFO,
    and the `?access_token=` form this server refuses is exactly what such a line carries.
    """
    from papra_suite.logging_setup import REDACTED, setup_logging

    token = mint_token(store, label="registry-check", org_ids=[ORG])
    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
    setup_logging()
    try:
        with client_for(app_for(store)) as client:
            initialize(client, token)      # the verifier runs here, and registers the plaintext
        logging.getLogger("some-http-client").info("GET /mcp?access_token=%s", token)
    finally:
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)
    printed = capsys.readouterr().out
    assert token not in printed, "an MCP token reached a log line in the clear"
    assert REDACTED in printed, "nothing was redacted, so the registry never saw the token"


def test_the_public_url_host_is_the_one_the_allow_list_uses(store):
    """A URL with a port and an uppercase host still accepts the host `fetch` sends."""
    app = build_mcp_app(store, OkPapra(), OkKreuzakt(), url="https://Papra-MCP.ts.net:8443/prefix")
    token = mint_token(store, label="case", org_ids=[ORG])
    with client_for(app) as client:
        response, _ = initialize(client, token, host=f"{HOST}:8443")
        bare = client.get("/mcp", headers=initialize_headers(token, host=HOST))
    assert response.status_code == 200
    assert bare.status_code != 421    # the bare host is on the list too, in lowercase


# --- `serve` gating and `check` (N5) -------------------------------------------------------

SERVE_ENV = {
    "PAPRA_ALLOWED_SENDERS": "sender@x.com",
    "PAPRA_BASE_URL": "http://papra:1221", "PAPRA_API_KEY": "k",
    "LLM_BASE_URL": "http://llm:8000", "LLM_API_KEY": "k",
    "TITLE_MODEL": "t", "OCR_MODEL": "o", "ADMIN_PASSWORD": "p",
    "SWEEP_INTERVAL_S": "0",
    "PAPRA_KREUZAKT_URL": "http://kreuzakt.test",
    "PAPRA_MCP_URL": PUBLIC_URL,
}


async def test_serve_does_not_start_the_mcp_listener_by_default(monkeypatch, tmp_path, store):
    """Off until a setting says otherwise, exactly as every write flag in this build shipped.

    The listener is the one part of this build that answers the network, so it lands shut: an
    upgrade must not hand a deployment a server nobody asked for, and the operator opens it with one
    settings row once they have minted a token.
    """
    import asyncio

    from papra_suite.config import load_config
    from papra_suite.pipeline import serve
    from tests.test_index_job import FakeKreuzakt, FakePapra

    started = []

    async def fake_listener(*args, **kwargs):
        started.append((args, kwargs))

    monkeypatch.setattr("papra_suite.mcp_server.run_mcp_listener", fake_listener)
    config = load_config({**SERVE_ENV, "DATA_DIR": str(tmp_path)})
    stop = asyncio.Event()
    stop.set()                      # start up and shut down again: nothing has to run

    await serve(config, stop=stop, store=store, papra=FakePapra(), kreuzakt=FakeKreuzakt())

    assert started == []


async def test_serve_starts_the_mcp_listener_only_when_the_setting_enables_it(
        monkeypatch, tmp_path, store):
    import asyncio

    from papra_suite.config import load_config
    from papra_suite.pipeline import serve
    from tests.test_index_job import FakeKreuzakt, FakePapra

    started = []

    async def fake_listener(*args, **kwargs):
        started.append((args, kwargs))

    monkeypatch.setattr("papra_suite.mcp_server.run_mcp_listener", fake_listener)
    store.set_setting(MCP_ENABLED_SETTING, "1")
    config = load_config({**SERVE_ENV, "DATA_DIR": str(tmp_path)})
    stop = asyncio.Event()
    stop.set()

    await serve(config, stop=stop, store=store, papra=FakePapra(), kreuzakt=FakeKreuzakt())

    assert len(started) == 1
    # The listener gets the store and the two clients `serve` already owns, so the MCP path cannot
    # open a second connection pool — or a second set of credentials — behind `serve`'s back.
    args, kwargs = started[0]
    assert args[0] is config and args[1] is store
    assert kwargs.get("papra") is not None and kwargs.get("kreuzakt") is not None


def test_check_prints_the_mcp_state_without_labels(monkeypatch, tmp_path, capsys):
    """`check` says whether a listener would run and how many credentials exist — never which."""
    from papra_suite.__main__ import main

    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["papra-suite", "check"])
    assert main() == 0
    out = capsys.readouterr().out
    assert "mcp: disabled" in out
    assert "0 active" in out and "0 revoked" in out

    # Labels name a device and an agent, and `check` output is what gets pasted into tickets; the
    # counts answer "is anything listening, and with how many keys" on their own.
    store = Store(tmp_path / "state.db")
    mint_token(store, label="hermes-laptop", org_ids=[ORG])
    mint_token(store, label="pi", org_ids=[ORG])
    revoke_token(store, label="pi")
    store.close()

    assert main() == 0
    out = capsys.readouterr().out
    assert "1 active" in out and "1 revoked" in out
    assert "hermes-laptop" not in out and "mcp_token" not in out


def test_check_names_the_bind_address_when_the_server_is_enabled(monkeypatch, tmp_path, capsys):
    from papra_suite.__main__ import main

    for key, value in BASE_ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PAPRA_MCP_URL", PUBLIC_URL)
    monkeypatch.setenv("PAPRA_MCP_PORT", "8765")
    store = Store(tmp_path / "state.db")
    store.migrate()
    store.set_setting(MCP_ENABLED_SETTING, "1")
    store.close()
    monkeypatch.setattr(sys, "argv", ["papra-suite", "check"])

    assert main() == 0
    out = capsys.readouterr().out
    assert "mcp: enabled" in out and "127.0.0.1:8765" in out
