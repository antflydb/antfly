"""Lossless Cypher values and named graph operations."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from .client_generated.models import (
    CommittedMutationOutcome,
    GraphCatalogEntry,
    GraphDefinition,
    GraphQueryRequest,
    GraphQueryResult,
)

if TYPE_CHECKING:
    from .client import AntflyClient


def encode_graph_parameter(value: Any, depth: int = 0) -> dict[str, Any]:
    """Encode data without rounding integers or reserving user map keys."""
    if depth > 64:
        raise ValueError("Graph parameter nesting exceeds 64 levels")
    if value is None:
        return {"type": "null", "value": None}
    if isinstance(value, bool):
        return {"type": "boolean", "value": value}
    if isinstance(value, int):
        if not -(1 << 63) <= value < (1 << 63):
            raise ValueError("Graph INTEGER exceeds signed 64-bit range")
        return {"type": "integer", "value": str(value)}
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Graph numbers must be finite")
        return {"type": "number", "value": value}
    if isinstance(value, str):
        return {"type": "string", "value": value}
    if isinstance(value, (list, tuple)):
        return {
            "type": "list",
            "value": [encode_graph_parameter(item, depth + 1) for item in value],
        }
    if isinstance(value, Mapping) and all(isinstance(key, str) for key in value):
        return {
            "type": "map",
            "value": {key: encode_graph_parameter(item, depth + 1) for key, item in value.items()},
        }
    raise ValueError("Graph parameters require plain data values and string map keys")


def decode_graph_value(value: Any) -> Any:
    """Decode data; entities retain their tagged identity and properties."""
    wire = value.to_dict() if hasattr(value, "to_dict") else value
    kind, payload = wire["type"], wire["value"]
    if kind == "integer":
        result = int(payload)
        if not isinstance(payload, str) or not -(1 << 63) <= result < (1 << 63):
            raise ValueError("Invalid graph INTEGER")
        return result
    if kind == "list":
        return [decode_graph_value(item) for item in payload]
    if kind == "map":
        return {key: decode_graph_value(item) for key, item in payload.items()}
    if kind in ("node", "relationship", "path"):
        return value
    if kind in ("null", "boolean", "number", "string"):
        return payload
    raise ValueError(f"Unsupported graph value {kind!r}")


class GraphOperations:
    def __init__(self, client: AntflyClient):
        self._client = client

    def list(self) -> list[GraphCatalogEntry]:
        return [GraphCatalogEntry.from_dict(value) for value in self._client._request("GET", "/db/v1/graphs")]

    def get(self, name: str) -> GraphCatalogEntry:
        return GraphCatalogEntry.from_dict(self._client._request("GET", f"/db/v1/graphs/{quote(name, safe='')}"))

    def create(
        self, name: str, definition: GraphDefinition | Mapping[str, Any]
    ) -> GraphCatalogEntry | CommittedMutationOutcome:
        body = definition.to_dict() if isinstance(definition, GraphDefinition) else dict(definition)
        return self._catalog_result(self._client._request("POST", f"/db/v1/graphs/{quote(name, safe='')}", json=body))

    def drop(self, name: str) -> None:
        self._client._request("DELETE", f"/db/v1/graphs/{quote(name, safe='')}")

    def update(
        self,
        name: str,
        expected_revision: str,
        definition: GraphDefinition | Mapping[str, Any],
    ) -> GraphCatalogEntry | CommittedMutationOutcome:
        """Conditionally replace mappings; never replay a stale revision."""
        body = definition.to_dict() if isinstance(definition, GraphDefinition) else dict(definition)
        if not isinstance(expected_revision, str) or re.fullmatch(r"[1-9][0-9]*", expected_revision) is None:
            raise ValueError("Expected a positive decimal graph revision")
        return self._catalog_result(
            self._client._request(
                "PUT",
                f"/db/v1/graphs/{quote(name, safe='')}",
                json={"expected_revision": expected_revision, "definition": body},
            )
        )

    @staticmethod
    def _catalog_result(
        value: dict[str, Any],
    ) -> GraphCatalogEntry | CommittedMutationOutcome:
        if value.get("status") == "committed_visibility_pending":
            return CommittedMutationOutcome.from_dict(value)
        return GraphCatalogEntry.from_dict(value)

    def execute(
        self,
        name: str,
        query: str | GraphQueryRequest,
        *,
        parameters: Mapping[str, Any] | None = None,
        write_request_id: str | None = None,
        limits: Mapping[str, int] | None = None,
    ) -> GraphQueryResult:
        """Execute once. Inspect a retained write ID after uncertain delivery."""
        if isinstance(query, GraphQueryRequest):
            if parameters is not None or write_request_id is not None or limits is not None:
                raise ValueError("Choose a request model or query options")
            body = query.to_dict()
        else:
            body = {"query": query}
            if parameters is not None:
                body["typed_parameters"] = encode_graph_parameter(parameters)
            if write_request_id is not None:
                self._validate_id(write_request_id)
                body["write_request_id"] = write_request_id
            if limits is not None:
                body["limits"] = dict(limits)
        if "parameters" in body and "typed_parameters" in body:
            raise ValueError("Choose parameters or typed_parameters")
        result = self._client._request("POST", f"/db/v1/graphs/{quote(name, safe='')}/query", json=body)
        if result.get("wire_version") != 1 or any(len(row) != len(result["columns"]) for row in result["rows"]):
            raise ValueError("Invalid graph result contract")
        return GraphQueryResult.from_dict(result)

    def write_status(self, transaction_id: str) -> dict[str, Any]:
        """Read a principal-bound receipt without replaying the statement."""
        self._validate_id(transaction_id)
        return self._client._request("GET", f"/db/v1/transactions/{transaction_id}/status")

    @staticmethod
    def _validate_id(value: str) -> None:
        if not re.fullmatch(r"[0-9a-fA-F]{32}", value):
            raise ValueError("Invalid graph write transaction ID")
