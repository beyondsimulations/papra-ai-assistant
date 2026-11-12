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


def _papra_is_up(*, readiness_timeout_s: float = 0.0) -> bool:
    # `docker compose up -d --wait` reports the container healthy as soon as it is
    # *running* — the papra image defines no healthcheck, so the app may still be
    # booting and refuse connections for a few seconds. Retry within a bounded
    # window so `make test` does not race the app startup.
    deadline = time.monotonic() + readiness_timeout_s
    while True:
        try:
            response = httpx.get(f"{PAPRA_TEST_URL}/api/api-keys/current", timeout=2.0)
            return response.status_code in (200, 401)
        except httpx.TransportError:
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.5)


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
