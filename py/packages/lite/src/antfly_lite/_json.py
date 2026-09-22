"""JSON request/response conventions shared by Database methods.

Convention (documented in the README): request parameters accept a dict,
list, str, or bytes; dict/list values are serialized with compact
separators. Response values are parsed JSON (dict/list/scalar) by default;
pass ``raw=True`` on any JSON-returning method to get the raw bytes instead.
"""

from __future__ import annotations

import json
from typing import Any

JSONInput = dict | list | str | bytes


def encode_json_input(value: JSONInput) -> bytes:
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def encode_text(value: str | bytes) -> bytes:
    """Encode a plain string/bytes argument (a key, name, kind, or similar)
    that is not itself a JSON payload."""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    return value.encode("utf-8")


def decode_json_response(data: bytes, raw: bool) -> Any:
    if raw:
        return data
    if not data:
        return None
    return json.loads(data)
