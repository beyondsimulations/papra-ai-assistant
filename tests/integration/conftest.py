import time
import uuid

import httpx
import pytest

PAPRA_TEST_URL = "http://127.0.0.1:12210"
API_KEY_PERMISSIONS = [
    "organizations:read",
    "documents:create",
    "documents:read",
    "documents:update",
    "tags:create",
    "tags:read",
]


def _probe_readiness(url: str, *, readiness_timeout_s: float,
                     poll_interval_s: float = 0.5) -> bool:
    # `docker compose up -d --wait` reports the container healthy as soon as it is
    # *running* — the papra image defines no healthcheck, so the app may still be
    # booting and refuse connections (or answer with a non-200/401 status, e.g.
    # 503 while booting) for a few seconds. Retry on ANY outcome — connection
    # error or HTTP status — within a bounded window; only after the full window
    # passes without a 200/401 do we report the stack as down.
    deadline = time.monotonic() + readiness_timeout_s
    while True:
        try:
            response = httpx.get(f"{url}/api/api-keys/current", timeout=2.0)
            if response.status_code in (200, 401):
                return True
        except httpx.TransportError:
            pass
        if time.monotonic() >= deadline:
            return False
        time.sleep(poll_interval_s)


def _papra_is_up(*, readiness_timeout_s: float = 0.0) -> bool:
    return _probe_readiness(PAPRA_TEST_URL, readiness_timeout_s=readiness_timeout_s)


@pytest.fixture(scope="session")
def papra_stack():
    if not _papra_is_up(readiness_timeout_s=15.0):
        pytest.skip("scratch Papra not running; use `make test`")

    # unique per session so a reused container cannot collide
    suffix = uuid.uuid4().hex[:8]
    with httpx.Client(base_url=PAPRA_TEST_URL) as http:
        sign_up = http.post(
            "/api/auth/sign-up/email",
            json={
                "email": f"seed-{suffix}@papra-suite.test",
                "password": "seed-password-123456",
                "name": "Seed User",
            },
        )
        sign_up.raise_for_status()

        org = http.post("/api/organizations", json={"name": f"Test Org {suffix}"})
        org.raise_for_status()
        org_id = org.json()["organization"]["id"]

        api_key = http.post(
            "/api/api-keys",
            json={"name": f"suite-{suffix}", "permissions": API_KEY_PERMISSIONS},
        )
        api_key.raise_for_status()
        token = api_key.json()["token"]

    yield PAPRA_TEST_URL, org_id, token
