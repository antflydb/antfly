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


def verify_generation_coverage(prior, closure, following):
    """Verify signed generation coverage only; no delivery or ordering claim."""
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


def verify_capture(capture, secret, issuer, coordinator, destination):
    """Bind a verified exchange to its single proxy connection observation."""
    if capture.get("event") != "proxy_response_capture":
        raise ValueError("not a connection capture")
    proof = verify_exchange(
        capture["request_message"],
        capture["response_message"],
        secret,
        issuer,
        coordinator,
        destination,
    )
    transport = capture.get("transport", {})
    if (
        not isinstance(transport.get("observer"), str)
        or not transport["observer"]
        or not isinstance(capture.get("proxy"), str)
        or type(capture.get("connection")) is not int
        or capture["connection"] <= 0
    ):
        raise ValueError("missing proxy observation identity")
    for key in (
        "request_forwarded_bytes",
        "response_forwarded_bytes",
        "response_dropped_bytes",
    ):
        if type(transport.get(key)) is not int or transport[key] < 0:
            raise ValueError("invalid forwarding count")
    for key in (
        "request_first_received_ns",
        "request_first_forwarded_ns",
        "response_last_forwarded_ns",
    ):
        if transport.get(key) is not None and (
            type(transport[key]) is not int or transport[key] <= 0
        ):
            raise ValueError("invalid forwarding time")
    if (
        transport["request_forwarded_bytes"]
        != capture["request_message"]["received_bytes"]
        or transport.get("request_first_received_ns") is None
        or transport.get("request_first_forwarded_ns") is None
        or transport["request_first_received_ns"]
        > transport["request_first_forwarded_ns"]
        or transport["response_forwarded_bytes"] + transport["response_dropped_bytes"]
        > capture["response_message"]["received_bytes"]
    ):
        raise ValueError("inconsistent connection forwarding")
    proof["observation"] = {
        **transport,
        "proxy": capture["proxy"],
        "connection": capture["connection"],
        "response_received_bytes": capture["response_message"]["received_bytes"],
    }
    return proof


def verify_generation_closure(prior, closure, following):
    """Require fence forwarding before the newer request reaches this relay.

    This proves proxy-observed transport order, not application consumption of
    the fence or retirement of the coordinator's durable record.
    """
    result = verify_generation_coverage(prior, closure, following)
    old, fence, new = (
        item.get("observation", {}) for item in (prior, closure, following)
    )
    if (
        not old
        or not fence
        or not new
        or any(
            item.get("observer") != fence.get("observer")
            or item.get("proxy") != fence.get("proxy")
            for item in (old, new)
        )
        or len({item.get("connection") for item in (old, fence, new)}) != 3
        or fence.get("response_dropped_bytes") != 0
        or fence.get("response_forwarded_bytes") != fence.get("response_received_bytes")
        or not fence.get("response_last_forwarded_ns")
        or not old.get("request_first_forwarded_ns", 0)
        < fence.get("request_first_forwarded_ns", 0)
        or not fence["request_first_forwarded_ns"]
        <= fence["response_last_forwarded_ns"]
        < new.get("request_first_received_ns", 0)
    ):
        raise ValueError("no complete fence forwarding before newer request")
    return {
        **result,
        "fence_forwarded_ns": fence["response_last_forwarded_ns"],
        "new_request_received_ns": new["request_first_received_ns"],
    }
