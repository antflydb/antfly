"""Native local-fixture workload attempt frames; authenticity is not retirement.

Byte framing matches api/workload_attempt_protocol.zig. Namespace/epoch changes
never retire debt here. Only a verified terminal or applicable fence can do so.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json


def framed(parts, digest):
    for part in parts:
        digest.update(len(part).to_bytes(8, "big"))
        digest.update(part)
    return digest.digest()


def encoded(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=")


def decoded(data):
    return base64.b64decode(
        data + b"=" * (-len(data) % 4), altchars=b"-_", validate=True
    )


def signature(secret, issuer, domain, payload):
    if len(secret.encode()) < 32 or not 1 <= len(issuer.encode()) <= 256:
        raise ValueError("invalid fixture signing keys")
    return framed(
        (domain.encode(), issuer.encode(), payload),
        hmac.new(secret.encode(), digestmod=hashlib.sha256),
    )


def sign(secret, issuer, domain, value):
    payload = encoded(json.dumps(value, separators=(",", ":")).encode())
    frame = payload + b"." + encoded(signature(secret, issuer, domain, payload))
    if len(frame) > 4096:
        raise ValueError("attempt frame exceeds protocol bound")
    return frame.decode()


def verify(secret, issuer, domain, frame):
    if len(frame) > 4096:
        raise ValueError("attempt frame exceeds protocol bound")
    payload, mac = frame.encode().split(b".")
    if not hmac.compare_digest(
        signature(secret, issuer, domain, payload), decoded(mac)
    ):
        raise ValueError("invalid workload evidence signature")
    return json.loads(decoded(payload))


def validate_attempt(attempt, version=3):
    fields = {
        "coordinator": 64,
        "generation": 64,
        "sequence": 64,
        "operation": 128,
        "destination": 64,
        "worker_incarnation": 64,
    }
    if version == 3:
        fields["worker_namespace"] = 128
    for key, bits in fields.items():
        if type(attempt.get(key)) is not int or not 0 < attempt[key] < 1 << bits:
            raise ValueError(f"invalid attempt {key}")
    if version not in (1, 2, 3):
        raise ValueError("unsupported declared workload protocol")


def sign_request(
    secret,
    issuer,
    attempt,
    method,
    path,
    body,
    remaining_ns,
    *,
    version=3,
    acknowledged_through=0,
):
    validate_attempt(attempt, version)
    if (
        not 0 < remaining_ns < 1 << 64
        or not 0 <= acknowledged_through < attempt["sequence"]
        or (version == 1 and acknowledged_through)
    ):
        raise ValueError("invalid remaining budget or acknowledgement watermark")
    digest = framed((method.encode(), path.encode(), body), hashlib.sha256())
    return sign(
        secret,
        issuer,
        "antfly-workload-request-v1",
        {
            "version": version,
            "attempt": attempt,
            "remaining_ns": remaining_ns,
            "request_digest": list(digest),
            "acknowledged_through": acknowledged_through,
        },
    )


def terminal(secret, issuer, frame, attempt, status, body, version=3):
    validate_attempt(attempt, version)
    value = verify(secret, issuer, "antfly-workload-terminal-v1", frame)
    if (
        value.get("version") != 1
        or value.get("attempt") != attempt
        or value.get("status") != status
        or value.get("response_digest") != list(hashlib.sha256(body).digest())
    ):
        raise ValueError("terminal identity/status/body mismatch")
    return value


def fence(secret, issuer, frame, attempt, version=3):
    validate_attempt(attempt, version)
    value = verify(secret, issuer, "antfly-workload-fence-v1", frame)
    if value.get("version") != 1 or any(
        value.get(field, 0) != attempt.get(field, 0)
        for field in (
            "coordinator",
            "destination",
            "worker_incarnation",
            "worker_namespace",
        )
    ):
        raise ValueError("fence membership/namespace/epoch mismatch")
    quiesced, fenced = value.get("quiesced_through"), value.get("fenced_through")
    if (
        type(quiesced) is not int
        or type(fenced) is not int
        or not attempt["generation"] <= quiesced <= fenced
    ):
        raise ValueError("fence does not prove requested generation quiescent")
    return value
