"""Measure one real Papra webhook delivery: payload shape, actor, signature.

Scope (operator decision 2026-11-17): the webhook receiver is out of v1, so this
probe measures only the two facts that a future receiver would live or die on,
plus the signature layout. Retry ladder, `webhookUrlAllowedHostnames` behaviour
and receiver robustness are NOT measured — they no longer gate any task.

Setup, in order:

    docker compose -f docker-compose.test.yml -p papra-ai-assistant stop
    docker compose -f docker-compose.test.yml -p kreuzakt-bridge up -d --wait
    .venv/bin/python docs/superpowers/probes/2026-11-17-kreuzakt-contracts/listener.py &
    .venv/bin/python docs/superpowers/probes/2026-11-17-kreuzakt-contracts/probe_webhooks.py

Papra's SSRF protection rejects `host.docker.internal` by default (the verbatim
rejection is captured below as `Q1_rejection_default_config`), so the container
needs one env var to accept a receiver on the host:

    WEBHOOK_URL_ALLOWED_HOSTNAMES=host.docker.internal

which is easiest to add through a throwaway override file kept out of the repo:

    printf 'services:\\n  papra:\\n    environment:\\n      WEBHOOK_URL_ALLOWED_HOSTNAMES: "host.docker.internal"\\n' > /tmp/wk-override.yml
    docker compose -f docker-compose.test.yml -f /tmp/wk-override.yml -p kreuzakt-bridge up -d --wait papra

Writes `capture-webhooks.json` next to this file and appends to `listener.jsonl`.

Not ruff-checked (docs/superpowers/probes/ is excluded in pyproject.toml).
"""

import base64
import hashlib
import hmac
import json
import secrets
import sys
import time
import uuid
from pathlib import Path

import httpx

BASE = "http://127.0.0.1:12210"
HOOK_PATH = "/hook"
RECEIVER_URL = f"http://host.docker.internal:{8099}{HOOK_PATH}"
UNLISTED_URL = "http://hook-not-allowlisted.invalid:8099/hook"
HERE = Path(__file__).parent
CAPTURE_PATH = HERE / "capture-webhooks.json"
LISTENER_LOG = HERE / "listener.jsonl"

capture = {}

EVENTS = [
    "document:created",
    "document:updated",
    "document:deleted",
    "document:tag:added",
    "document:tag:removed",
]

PERMISSIONS = [
    "organizations:read",
    "documents:create",
    "documents:read",
    "documents:update",
    "documents:delete",
    "tags:create",
    "tags:read",
]

PDF = (HERE.parent / "2026-11-17-kreuzakt" / "sample.pdf").read_bytes()


def listener_deliveries():
    if not LISTENER_LOG.exists():
        return []
    return [json.loads(line) for line in LISTENER_LOG.read_text().splitlines() if line.strip()]


def wait_for_deliveries(since, *, timeout_s=45.0):
    """Deliveries appended after the first `since` line, oldest first."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        rows = listener_deliveries()[since:]
        if rows:
            time.sleep(1.0)  # let a burst land: Papra may deliver several events together
            return listener_deliveries()[since:]
        time.sleep(0.5)
    return []


def listener_is_up():
    try:
        httpx.get("http://127.0.0.1:8099/", timeout=3.0)
    except httpx.TransportError:
        return False
    return True


def start_listener():
    """Start listener.py as a child process unless one is already answering.

    Self-contained on purpose: the receiver and the trigger have to be observed
    in one run, and a receiver started from an earlier shell is the kind of
    invisible state that makes a measurement unreproducible.
    """
    import subprocess

    log = open(HERE / "listener.out", "w", encoding="utf-8")
    child = subprocess.Popen(
        [sys.executable, str(HERE / "listener.py"), "8099"],
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if listener_is_up():
            return child
        time.sleep(0.3)
    child.terminate()
    raise RuntimeError("listener.py did not come up on :8099")


def main():
    child = None
    if not listener_is_up():
        child = start_listener()
    session = httpx.Client(base_url=BASE, timeout=30.0)
    suffix = uuid.uuid4().hex[:8]
    sign_up = session.post(
        "/api/auth/sign-up/email",
        json={
            "email": f"hook-{suffix}@papra-suite.test",
            "password": "seed-password-123456",
            "name": "Hook Probe",
        },
    )
    capture["signup"] = {"status": sign_up.status_code}
    user_token = sign_up.json()["token"]
    session_headers = {"Authorization": f"Bearer {user_token}"}
    org = session.post("/api/organizations", json={"name": f"Hook Org {suffix}"}, headers=session_headers)
    org_id = org.json()["organization"]["id"]
    capture["organization_id"] = org_id

    # --- Q1: does POST /api/organizations/{org}/webhooks take {"url","events"}? ----
    wrong_shape = session.post(
        f"/api/organizations/{org_id}/webhooks",
        headers=session_headers,
        json={"url": RECEIVER_URL, "events": ["document.updated"]},
    )
    capture["Q1_creation_with_dotted_event_and_no_name"] = {
        "request": {"url": RECEIVER_URL, "events": ["document.updated"]},
        "status": wrong_shape.status_code,
        "response": wrong_shape.json(),
    }
    blocked = session.post(
        f"/api/organizations/{org_id}/webhooks",
        headers=session_headers,
        json={"name": "blocked-probe", "url": UNLISTED_URL, "events": ["document:updated"]},
    )
    capture["Q1_rejection_default_config"] = {
        "request_url": UNLISTED_URL,
        "status": blocked.status_code,
        "response": blocked.json(),
    }

    # A Standard-Webhooks secret: `whsec_` + base64 of 32 random bytes.
    secret = "whsec_" + base64.b64encode(secrets.token_bytes(32)).decode()
    created = session.post(
        f"/api/organizations/{org_id}/webhooks",
        headers=session_headers,
        json={"name": "probe", "url": RECEIVER_URL, "events": EVENTS, "secret": secret},
    )
    capture["Q1_creation_accepted"] = {
        "request": {"name": "probe", "url": RECEIVER_URL, "events": EVENTS, "secret": "<32-byte hex>"},
        "status": created.status_code,
        "response": created.json(),
    }
    webhook_id = created.json().get("webhook", {}).get("id")
    capture["webhook_id"] = webhook_id

    # An API key in a client that also holds a session cookie is rejected as
    # ambiguous credentials, so API-key calls get their own cookie-free client.
    api_key = session.post("/api/api-keys", json={"name": f"hook-{suffix}", "permissions": PERMISSIONS})
    token = api_key.json()["token"]
    api_key_id = api_key.json()["apiKey"]["id"]
    key = httpx.Client(base_url=BASE, timeout=30.0, headers={"Authorization": f"Bearer {token}"})

    doc = key.post(
        f"/api/organizations/{org_id}/documents",
        files={"file": ("hook-probe.pdf", PDF, "application/pdf")},
    )
    capture["document_create"] = {"status": doc.status_code, "body": doc.json()}
    print("webhook create status:", created.status_code, "doc create:", doc.status_code, flush=True)
    document_id = doc.json()["document"]["id"]

    mark = len(listener_deliveries())
    renamed = key.patch(
        f"/api/organizations/{org_id}/documents/{document_id}",
        json={"name": "Renamed By API Key.pdf"},
    )
    capture["api_key_rename"] = {"status": renamed.status_code}
    api_key_events = wait_for_deliveries(mark)

    # Same change, made with a browser-style session instead of a key: the only
    # way to learn whether a receiver could ever tell the two apart.
    mark = len(listener_deliveries())
    human = session.patch(
        f"/api/organizations/{org_id}/documents/{document_id}",
        headers=session_headers,
        json={"name": "Renamed By Human.pdf"},
    )
    capture["session_rename"] = {"status": human.status_code}
    session_events = wait_for_deliveries(mark)

    mark = len(listener_deliveries())
    tag = key.post(f"/api/organizations/{org_id}/tags", json={"name": f"probe-{suffix}"})
    tag_id = tag.json().get("tag", {}).get("id")
    capture["tag_create"] = {"status": tag.status_code, "body": tag.json()}
    tag_events = []
    if tag_id:
        tag_added = key.post(
            f"/api/organizations/{org_id}/documents/{document_id}/tags", json={"tagId": tag_id}
        )
        capture["tag_add"] = {"status": tag_added.status_code, "body": tag_added.json()}
        tag_events = wait_for_deliveries(mark)
    else:
        capture["tag_add"] = {"skipped": "tag creation failed, nothing to attach"}

    mark = len(listener_deliveries())
    deleted = key.delete(f"/api/organizations/{org_id}/documents/{document_id}")
    capture["document_delete"] = {"status": deleted.status_code}
    delete_events = wait_for_deliveries(mark)

    def summarise(rows):
        return [
            {
                "event_field": row["body"].get("event") if isinstance(row["body"], dict) else None,
                "top_level_keys": sorted(row["body"].keys()) if isinstance(row["body"], dict) else None,
                "path": row["path"],
                "header_names": sorted(row["headers"].keys()),
            }
            for row in rows
        ]

    capture["Q2_api_key_rename_deliveries"] = api_key_events
    capture["Q3_session_rename_deliveries"] = session_events
    capture["tag_added_deliveries"] = tag_events
    capture["deleted_deliveries"] = delete_events
    capture["delivery_summary"] = {
        "api_key_rename": summarise(api_key_events),
        "session_rename": summarise(session_events),
        "tag_added": summarise(tag_events),
        "deleted": summarise(delete_events),
    }

    # --- Q4: signature header and the exact signed string -----------------------
    def b64hmac(key: bytes, message: bytes) -> str:
        return base64.b64encode(hmac.new(key, message, hashlib.sha256).digest()).decode()

    checks = []
    for row in api_key_events + session_events + tag_events + delete_events:
        raw = row.get("raw_body", "").encode()
        headers = {k.lower(): v for k, v in row["headers"].items()}
        signature = headers.get("webhook-signature", "")
        msg_id = headers.get("webhook-id", "")
        stamp = headers.get("webhook-timestamp", "")
        # The secret we supplied is `whsec_` + hex. Standard Webhooks secrets are
        # base64, so try the secret both ways: raw bytes, and base64-decoded.
        secret_b64 = secret.split("_", 1)[1]
        keys = [("secret.raw", secret.encode())]
        for pad in ("", "=", "=="):
            try:
                keys.append(("secret.base64decoded", base64.b64decode(secret_b64 + pad)))
                break
            except Exception:  # noqa: BLE001 - a wrong length just means "not base64"
                continue
        candidates = {
            "plain hex hmac(secret, body)": [
                hmac.new(k, raw, hashlib.sha256).hexdigest() for _, k in keys
            ],
            "standard-webhooks v1 (id.timestamp.body)": [
                "v1," + b64hmac(k, f"{msg_id}.{stamp}.".encode() + raw) for _, k in keys
            ],
            "v1 with timestamp.body only": [
                "v1," + b64hmac(k, f"{stamp}.".encode() + raw) for _, k in keys
            ],
        }
        matched = [label for label, values in candidates.items() if any(v == signature for v in values)]
        matched_key = [
            f"{label} + {key_label}"
            for label, values in candidates.items()
            for (key_label, value) in zip([k for k, _ in keys], values)
            if value == signature
        ]
        checks.append(
            {
                "event": row["body"].get("type") if isinstance(row["body"], dict) else None,
                "secret_forms_tried": [label for label, _ in keys],
                "matched_key_form": matched_key,
                "signature_header_name": "webhook-signature" if signature else None,
                "signature_value": signature,
                "other_webhook_headers": {k: v for k, v in headers.items() if k.startswith("webhook-")},
                "raw_body_bytes_recovered": headers.get("content-length") == str(len(raw)),
                "matched_formula": matched,
                "plain_hex_hmac_reproduces_it": any(
                    v == signature for v in candidates["plain hex hmac(secret, body)"]
                ),
            }
        )
    capture["Q4_signature_checks"] = checks

    capture["listener_raw_line_count"] = len(listener_deliveries())

    if webhook_id:
        removed = session.delete(f"/api/organizations/{org_id}/webhooks/{webhook_id}", headers=session_headers)
        capture["webhook_delete"] = {"status": removed.status_code}

    CAPTURE_PATH.write_text(json.dumps(capture, indent=2, default=str))
    if child is not None:
        child.terminate()
    print("webhook create status:", created.status_code, "doc create:", doc.status_code, flush=True)
    print(json.dumps(capture["Q1_creation_with_dotted_event_and_no_name"], indent=2)[:1200])
    print(json.dumps(capture["Q1_rejection_default_config"], indent=2)[:600])
    print(json.dumps(capture["Q1_creation_accepted"], indent=2)[:1200])
    print(json.dumps(capture["delivery_summary"], indent=2)[:3000])
    print(json.dumps(capture["Q4_signature_checks"], indent=2)[:2500])
    print("\ncaptures written to", CAPTURE_PATH)


if __name__ == "__main__":
    main()
