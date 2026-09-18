"""Verify complete, connection-associated local proxy evidence without inferring debt."""

from __future__ import annotations

import base64
import hashlib
import json

import workload_attempt_evidence as protocol


def capture_message(prefix, received_bytes, secrets=()):
    """Keep allowlisted headers and exact body only for one complete HTTP message."""
    head, separator, body = prefix.partition(b"\r\n\r\n")
    result = {"complete": False, "received_bytes": received_bytes}
    if not separator or received_bytes != len(prefix):
        return result
    try:
        lines = head.decode("ascii").split("\r\n")
        headers = {}
        for line in lines[1:]:
            key, colon, value = line.partition(":")
            key = key.lower()
            if not colon or key in headers:
                return result
            headers[key] = value.strip()
        if "transfer-encoding" in headers:
            return result
        length = int(headers.get("content-length", "0"))
        if length != len(body) or length < 0:
            return result
        if any(secret.encode() in body or secret in lines[0] for secret in secrets):
            return result
        allowed = {
            "content-length",
            "content-type",
            "x-antfly-workload-attempt",
            "x-antfly-workload-evidence",
        }
        retained = {key: value for key, value in headers.items() if key in allowed}
        if any(secret in value for secret in secrets for value in retained.values()):
            return result
        return {
            **result,
            "complete": True,
            "start_line": lines[0],
            "headers": retained,
            "body_base64": base64.b64encode(body).decode(),
        }
    except (ValueError, UnicodeError):
        return result


def verify_exchange(request, response, secret, issuer, coordinator, destination):
    """Require HMAC request binding before checking terminal/fence/discovery proof."""
    if not request.get("complete") or not response.get("complete"):
        raise ValueError("incomplete, truncated, or pipelined capture")
    method, path, version = request["start_line"].split(" ")
    response_version, status_text, _ = response["start_line"].split(" ", 2)
    if version != "HTTP/1.1" or response_version != "HTTP/1.1":
        raise ValueError("unsupported capture framing")
    status = int(status_text)
    body = base64.b64decode(request["body_base64"], validate=True)
    response_body = base64.b64decode(response["body_base64"], validate=True)
    evidence = response["headers"]["x-antfly-workload-evidence"]
    frame = request["headers"].get("x-antfly-workload-attempt")
    if frame is None:
        # Discovery itself is authenticated by a nonce-bound response. The
        # retained request intentionally omits its service credential.
        query = json.loads(body)
        if (
            method != "POST"
            or path != "/internal/v1/workload/control"
            or query.get("workload_attempt_control") != "discover"
        ):
            raise ValueError("unsigned non-discovery request")
        value = protocol.verify(
            secret, issuer, "antfly-workload-discovery-v1", evidence
        )
        if status != 200 or any(
            value.get(key) != expected
            for key, expected in {
                "version": 1,
                "protocol_version": 3,
                "coordinator": coordinator,
                "destination": destination,
                "nonce": query["nonce"],
            }.items()
        ):
            raise ValueError("discovery status/identity/nonce mismatch")
        for key, bits in (("worker_namespace", 128), ("worker_incarnation", 64)):
            if type(value.get(key)) is not int or not 0 < value[key] < 1 << bits:
                raise ValueError("invalid discovery worker identity")
        return {"kind": "discovery", "proof": value}
    signed = protocol.verify(secret, issuer, "antfly-workload-request-v1", frame)
    attempt = signed["attempt"]
    protocol.validate_attempt(attempt, 3)
    expected_digest = list(
        protocol.framed((method.encode(), path.encode(), body), hashlib.sha256())
    )
    if (
        signed.get("version") != 3
        or signed.get("request_digest") != expected_digest
        or attempt["coordinator"] != coordinator
        or attempt["destination"] != destination
    ):
        raise ValueError("request digest/protocol/membership mismatch")
    control = (
        json.loads(body)
        if method == "POST" and path == "/internal/v1/workload/control"
        else {}
    )
    if control.get("workload_attempt_control") == "close_generation":
        if status != 200:
            raise ValueError("fence response status mismatch")
        proof = protocol.fence(secret, issuer, evidence, attempt)
        return {"kind": "fence", "attempt": attempt, "proof": proof}
    proof = protocol.terminal(secret, issuer, evidence, attempt, status, response_body)
    return {"kind": "terminal", "attempt": attempt, "status": status, "proof": proof}


def verify_generation_closure(prior, closure, following):
    """A matching signed fence must cover prior attempt before a newer send."""
    old, fence, new = prior["attempt"], closure["proof"], following["attempt"]
    if (
        closure["kind"] != "fence"
        or any(
            old[key] != fence[key] or old[key] != new[key]
            for key in (
                "coordinator",
                "destination",
                "worker_namespace",
                "worker_incarnation",
            )
        )
        or not old["generation"]
        <= fence["quiesced_through"]
        <= fence["fenced_through"]
        < new["generation"]
    ):
        raise ValueError("no matching generation closure before newer attempt")
    return {
        "closed_generation": old["generation"],
        "new_generation": new["generation"],
        "proof": fence,
    }
