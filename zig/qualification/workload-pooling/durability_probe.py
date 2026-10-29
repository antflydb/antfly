"""Verify an acknowledged changing postimage through HTTP, pgwire and reopen."""

import hashlib
import http.client
import json
import secrets
from pgwire_peer import Peer


ROW_ID = 9007199254740993
STATEMENT = f"SELECT id, payload FROM pooling_qualification WHERE id = {ROW_ID}"


def request(port, authorization, path, body):
    peer = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        peer.request(
            "POST",
            path,
            json.dumps(body),
            headers={
                "Authorization": authorization,
                "Content-Type": "application/json",
            },
        )
        response = peer.getresponse()
        payload = response.read()
        if response.status not in (200, 201):
            raise RuntimeError("durability probe request was rejected")
        result = json.loads(payload)
        if not isinstance(result, dict) or "error" in result or result.get("errors"):
            raise RuntimeError("durability probe returned a logical error")
        return result
    finally:
        peer.close()


def verify(api_port, sql_port, authorization, password, expected):
    result = request(
        api_port, authorization, "/db/v1/sql", {"statement": STATEMENT, "limit": 2}
    )
    columns = [column.get("name") for column in result.get("columns", [])]
    rows = result.get("rows")
    if columns != ["id", "payload"] or rows != [[str(ROW_ID), expected]]:
        raise RuntimeError("HTTP lost the acknowledged exact postimage")
    peer = Peer("127.0.0.1", sql_port, password=password)
    try:
        if peer.query(STATEMENT) != [[str(ROW_ID), expected]]:
            raise RuntimeError("pgwire lost the acknowledged exact postimage")
    finally:
        peer.close()
    return {
        "id": str(ROW_ID),
        "payload_bytes": len(expected.encode()),
        "payload_sha256": hashlib.sha256(expected.encode()).hexdigest(),
        "http_verified": True,
        "pgwire_verified": True,
    }


def write(api_port, sql_port, authorization, password, payload_bytes):
    marker = "durable:" + secrets.token_hex(16)
    expected = marker + "x" * max(0, payload_bytes - len(marker))
    request(
        api_port,
        authorization,
        "/db/v1/tables/pooling_qualification/batch",
        {
            "inserts": {"row-0": {"id": ROW_ID, "payload": expected}},
            "sync_level": "write",
        },
    )
    return expected, verify(api_port, sql_port, authorization, password, expected)
