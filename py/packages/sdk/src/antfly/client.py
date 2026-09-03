"""Main client interface for Antfly SDK."""

import base64
import json
import math
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any, TypeAlias, cast
from urllib.parse import quote

from httpx import Response, Timeout

from antfly.client_generated import Client
from antfly.client_generated.api.data_operations import (
    lookup_key,
)
from antfly.client_generated.client import AuthenticatedClient
from antfly.client_generated.models import (
    BatchRequest,
    BatchRequestInserts,
    CreateAlgebraicIndexRequest,
    CreatedAlgebraicIndex,
    CreatedEmbeddingsIndex,
    CreatedFullTextIndex,
    CreatedGraphIndex,
    CreateEmbeddingsIndexRequest,
    CreateFullTextIndexRequest,
    CreateGraphIndexRequest,
    ManagedEmbedderConfig as EmbedderConfig,
    ManagedEmbedderProvider as EmbedderProvider,
    Error,
    GraphKShortestPathsQuery,
    GraphMatchQuery,
    GraphQueries,
    GraphShortestPathQuery,
    GraphTraverseQuery,
    InferenceGenerateChunk,
    InferenceGenerateRequest,
    InferenceGenerateResponse,
    QueryResponses,
)
from antfly.client_generated.types import UNSET

from .exceptions import (
    AntflyException,
    IndexMutationTemporarilyUnavailableError,
    InferenceAPIError,
    InferenceCapacityError,
    StorageResourceExhaustedError,
)
from .graph_queries import require_graph_identifier as _require_graph_identifier
from .graph_results import decode_query_responses
from .index_config import validate_create_index_request_relationships

DEFAULT_WRITE_MAX_REQUEST_BYTES = 64 << 20
DEFAULT_MAX_JSON_RESPONSE_BYTES = 64 << 20
DEFAULT_MAX_ERROR_RESPONSE_BYTES = 1 << 20
INDEX_MUTATION_TEMPORARILY_UNAVAILABLE_CODES = frozenset(
    {"index_capability_upgrade_pending", "index_probe_unavailable"}
)
MAX_INFERENCE_ERROR_BYTES = 1 << 20
MAX_GENERATION_RESPONSE_BYTES = 16 << 20
MAX_GENERATION_SSE_EVENT_BYTES = 16 << 20
MAX_GENERATION_SSE_LINE_BYTES = 16 << 20
MAX_GRAPH_EDGE_TYPES = 64
MAX_GRAPH_EDGE_TYPE_UTF8_BYTES = 64 << 10
MAX_GRAPH_MATCH_QUERIES = 8

CreateIndexRequest: TypeAlias = (
    CreateFullTextIndexRequest | CreateEmbeddingsIndexRequest | CreateGraphIndexRequest | CreateAlgebraicIndexRequest
)
CreatedIndex: TypeAlias = CreatedFullTextIndex | CreatedEmbeddingsIndex | CreatedGraphIndex | CreatedAlgebraicIndex
GraphQueryInput: TypeAlias = GraphMatchQuery | GraphTraverseQuery | GraphShortestPathQuery | GraphKShortestPathsQuery
GraphQueriesInput: TypeAlias = GraphQueries | Mapping[str, GraphQueryInput | Mapping[str, Any]]
_CREATE_INDEX_REQUEST_TYPES = (
    CreateFullTextIndexRequest,
    CreateEmbeddingsIndexRequest,
    CreateGraphIndexRequest,
    CreateAlgebraicIndexRequest,
)
_CREATED_INDEX_TYPES = {
    "full_text": CreatedFullTextIndex,
    "embeddings": CreatedEmbeddingsIndex,
    "graph": CreatedGraphIndex,
    "algebraic": CreatedAlgebraicIndex,
}

MAX_GRAPH_HYDRATED_BINDINGS = 10_000


def _require_graph_table_qualifier(value: object, path: str) -> None:
    if not isinstance(value, str) or not any(char not in " \t\r\n" for char in value):
        raise AntflyException(f"{path} must contain a non-whitespace character")


def _validate_graph_hydration(value: Mapping[str, Any], path: str, *, binding_count: int | None = None) -> None:
    if "fields" in value and value.get("include_documents") is not True:
        raise AntflyException(f"{path}.fields requires include_documents=true")
    if binding_count is None or value.get("include_documents") is not True:
        return
    raw_limit = value.get("limit", 100)
    if type(raw_limit) is int and raw_limit * binding_count > MAX_GRAPH_HYDRATED_BINDINGS:
        raise AntflyException(
            f"{path} hydration requests {raw_limit * binding_count} binding documents; "
            f"the maximum is {MAX_GRAPH_HYDRATED_BINDINGS}"
        )


def _require_graph_edge_types(value: object, path: str) -> None:
    if value is None:
        return
    if not isinstance(value, list) or len(value) > MAX_GRAPH_EDGE_TYPES:
        raise AntflyException(f"{path} must contain at most {MAX_GRAPH_EDGE_TYPES} edge types")
    seen: set[str] = set()
    total_bytes = 0
    for index, edge_type in enumerate(value):
        if not isinstance(edge_type, str) or not edge_type:
            raise AntflyException(f"{path}[{index}] must be a non-empty valid UTF-8 string")
        try:
            encoded_len = len(edge_type.encode("utf-8"))
        except UnicodeEncodeError as exc:
            raise AntflyException(f"{path}[{index}] must be a non-empty valid UTF-8 string") from exc
        if edge_type in seen:
            raise AntflyException(f"{path} must not contain duplicate edge types")
        seen.add(edge_type)
        total_bytes += encoded_len
        if total_bytes > MAX_GRAPH_EDGE_TYPE_UTF8_BYTES:
            raise AntflyException(f"{path} must encode to at most {MAX_GRAPH_EDGE_TYPE_UTF8_BYTES} UTF-8 bytes")


def _validate_graph_edge_weight(value: Mapping[str, Any], path: str) -> None:
    if "edge_weight" not in value:
        return
    raw_range = value["edge_weight"]
    if not isinstance(raw_range, Mapping):
        raise AntflyException(f"{path}.edge_weight must be an object with min and/or max")
    if not raw_range or any(field not in {"min", "max"} for field in raw_range):
        raise AntflyException(f"{path}.edge_weight must contain min and/or max only")
    bounds: dict[str, float] = {}
    for field, label in (("min", "minimum"), ("max", "maximum")):
        if field not in raw_range:
            continue
        bound = raw_range[field]
        if isinstance(bound, bool) or not isinstance(bound, (int, float)):
            raise AntflyException(f"{path}.edge_weight.{field} must be a finite non-negative number")
        try:
            normalized = float(bound)
        except (OverflowError, ValueError) as exc:
            raise AntflyException(f"{path}.edge_weight.{field} must be a finite non-negative number") from exc
        if not math.isfinite(normalized) or normalized < 0:
            raise AntflyException(f"{path}.edge_weight.{field} must be a finite non-negative number")
        bounds[label] = normalized
    if bounds.get("minimum", 0) > bounds.get("maximum", math.inf):
        raise AntflyException(f"{path}.edge_weight.min must not exceed edge_weight.max")


def _validate_graph_path_objective(value: Mapping[str, Any], path: str) -> None:
    if "objective" not in value:
        return
    objective = value["objective"]
    if objective not in {"min_hops", "min_weight_sum", "max_weight_product"}:
        raise AntflyException(f"{path}.objective must be min_hops, min_weight_sum, or max_weight_product")


def _validate_graph_edges(edges: object, path: str) -> None:
    if not isinstance(edges, list):
        return
    for index, edge in enumerate(edges):
        if not isinstance(edge, Mapping):
            continue
        edge_path = f"{path}[{index}]"
        _require_graph_identifier(edge.get("from"), f"{edge_path}.from")
        _require_graph_identifier(edge.get("to"), f"{edge_path}.to")
        _validate_graph_direction(edge, edge_path)
        _require_graph_edge_types(edge.get("types"), f"{edge_path}.types")
        _validate_graph_edge_weight(edge, edge_path)


def _validate_graph_direction(value: Mapping[str, Any], path: str) -> None:
    if "direction" not in value:
        return
    direction = value["direction"]
    if not isinstance(direction, str) or direction not in {"out", "in", "both"}:
        raise AntflyException(f"{path}.direction must be out, in, or both")


def _validate_graph_match_identifiers(match: Mapping[str, Any], result: object, path: str) -> None:
    _require_graph_identifier(match.get("anchor"), f"{path}.match.anchor")

    nodes = match.get("nodes")
    if isinstance(nodes, Mapping):
        for alias, node in nodes.items():
            _require_graph_identifier(alias, f"{path}.match.nodes key")
            if isinstance(node, Mapping) and "table" in node:
                _require_graph_table_qualifier(node["table"], f"{path}.match.nodes[{alias!r}].table")

    edge_groups: list[tuple[object, str]] = [(match.get("edges"), f"{path}.match.edges")]
    where_groups: list[tuple[object, str, int]] = [(match.get("where"), f"{path}.match.where", 0)]
    optional = match.get("optional")
    if isinstance(optional, list):
        for index, optional_match in enumerate(optional):
            if not isinstance(optional_match, Mapping):
                continue
            optional_path = f"{path}.match.optional[{index}]"
            optional_nodes = optional_match.get("nodes")
            if isinstance(optional_nodes, Mapping):
                for alias, node in optional_nodes.items():
                    _require_graph_identifier(alias, f"{optional_path}.nodes key")
                    if isinstance(node, Mapping) and "table" in node:
                        _require_graph_table_qualifier(node["table"], f"{optional_path}.nodes[{alias!r}].table")
            edge_groups.append((optional_match.get("edges"), f"{optional_path}.edges"))
            where_groups.append((optional_match.get("where"), f"{optional_path}.where", 0))

    for edges, edges_path in edge_groups:
        _validate_graph_edges(edges, edges_path)

    while where_groups:
        where, where_path, depth = where_groups.pop()
        if where is None or not isinstance(where, Mapping):
            continue
        if depth >= 16:
            raise AntflyException(f"{where_path} exceeds the maximum graph predicate depth")
        conjunction = where.get("and")
        if isinstance(conjunction, list):
            where_groups.extend(
                (child, f"{where_path}.and[{index}]", depth + 1) for index, child in enumerate(conjunction)
            )
        not_equal = where.get("not_equal")
        if isinstance(not_equal, Mapping):
            for side in ("left", "right"):
                operand = not_equal.get(side)
                if isinstance(operand, Mapping):
                    _require_graph_identifier(operand.get("alias"), f"{where_path}.not_equal.{side}.alias")
        not_exists = where.get("not_exists")
        if isinstance(not_exists, Mapping):
            _validate_graph_edges(not_exists.get("edges"), f"{where_path}.not_exists.edges")

    if not isinstance(result, Mapping):
        return
    bindings = result.get("bindings")
    if isinstance(bindings, list):
        for index, alias in enumerate(bindings):
            _require_graph_identifier(alias, f"{path}.return.bindings[{index}]")
        _validate_graph_hydration(result, f"{path}.return", binding_count=len(bindings))
    aggregates = result.get("aggregates")
    if isinstance(aggregates, Mapping):
        for name, aggregate in aggregates.items():
            _require_graph_identifier(name, f"{path}.return.aggregates key")
            if not isinstance(aggregate, Mapping):
                continue
            count = aggregate.get("count")
            if count == "*":
                if "distinct" in aggregate:
                    raise AntflyException(f"{path}.return.aggregates[{name!r}].distinct is only valid for alias counts")
            else:
                _require_graph_identifier(count, f"{path}.return.aggregates[{name!r}].count")


def _serialize_graph_queries(graph_queries: GraphQueriesInput) -> dict[str, Any]:
    """Serialize typed canonical graph operations while preserving raw-map compatibility."""
    operations = graph_queries.to_dict() if isinstance(graph_queries, GraphQueries) else graph_queries
    if not operations:
        raise AntflyException("graph_queries must contain at least one named operation")
    if len(operations) > 64:
        raise AntflyException("graph_queries accepts at most 64 named operations")

    encoded: dict[str, Any] = {}
    match_queries = 0
    typed_queries = (GraphMatchQuery, GraphTraverseQuery, GraphShortestPathQuery, GraphKShortestPathsQuery)
    for name, query in operations.items():
        _require_graph_identifier(name, "graph_queries key")
        if isinstance(query, typed_queries):
            encoded_query = query.to_dict()
        elif isinstance(query, Mapping):
            encoded_query = dict(query)
        else:
            raise AntflyException(f"graph query {name!r} must be a generated graph query model or mapping")
        match = encoded_query.get("match")
        if isinstance(match, Mapping):
            match_queries += 1
            if match_queries > MAX_GRAPH_MATCH_QUERIES:
                raise AntflyException(f"graph_queries accepts at most {MAX_GRAPH_MATCH_QUERIES} match operations")
            _validate_graph_match_identifiers(match, encoded_query.get("return"), f"graph_queries[{name!r}]")
        traverse = encoded_query.get("traverse")
        if isinstance(traverse, Mapping):
            _validate_graph_direction(traverse, f"graph_queries[{name!r}].traverse")
            _require_graph_edge_types(traverse.get("edge_types"), f"graph_queries[{name!r}].traverse.edge_types")
            _validate_graph_edge_weight(traverse, f"graph_queries[{name!r}].traverse")
            start = traverse.get("start")
            if isinstance(start, Mapping) and isinstance(start.get("identities"), list):
                for index, identity in enumerate(start["identities"]):
                    if isinstance(identity, Mapping) and "table" in identity:
                        _require_graph_table_qualifier(
                            identity["table"],
                            f"graph_queries[{name!r}].traverse.start.identities[{index}].table",
                        )
            if isinstance(start, Mapping) and "result_ref" in start:
                path = f"graph_queries[{name!r}].traverse.start"
                result_ref = start.get("result_ref")
                if result_ref != "$query_results":
                    prefix = "$graph_results."
                    if not isinstance(result_ref, str) or not result_ref.startswith(prefix):
                        raise AntflyException(
                            f"{path}.result_ref must be $query_results or $graph_results.<query-name>"
                        )
                    _require_graph_identifier(result_ref[len(prefix) :], f"{path}.result_ref query name")
                binding = start.get("binding")
                if binding is not None:
                    if result_ref == "$query_results":
                        raise AntflyException(f"{path}.binding requires a $graph_results.<query-name> reference")
                    _require_graph_identifier(binding, f"{path}.binding")
            _validate_graph_hydration(traverse, f"graph_queries[{name!r}].traverse")
        for operation in ("shortest_path", "k_shortest_paths"):
            path_query = encoded_query.get(operation)
            if isinstance(path_query, Mapping):
                _validate_graph_direction(path_query, f"graph_queries[{name!r}].{operation}")
                operation_path = f"graph_queries[{name!r}].{operation}"
                _validate_graph_edge_weight(path_query, operation_path)
                _validate_graph_path_objective(path_query, operation_path)
                _require_graph_edge_types(
                    path_query.get("edge_types"),
                    f"graph_queries[{name!r}].{operation}.edge_types",
                )
                for endpoint in ("from", "to"):
                    identity = path_query.get(endpoint)
                    if isinstance(identity, Mapping) and "table" in identity:
                        _require_graph_table_qualifier(
                            identity["table"],
                            f"graph_queries[{name!r}].{operation}.{endpoint}.table",
                        )
                _validate_graph_hydration(path_query, operation_path)
        encoded[name] = encoded_query
    return encoded


def antfly_embedder(model: str, *, api_url: str | None = None) -> EmbedderConfig:
    """Build a typed Antfly inference embedder configuration."""
    config = EmbedderConfig(provider=EmbedderProvider.ANTFLY)
    config["model"] = model
    if api_url is not None:
        config["api_url"] = api_url
    return config


def _read_limited_response(response: Response, max_bytes: int) -> tuple[bytes, bool]:
    body = bytearray()
    for chunk in response.iter_bytes():
        remaining = max_bytes - len(body)
        if len(chunk) > remaining:
            body.extend(chunk[:remaining])
            return bytes(body), True
        body.extend(chunk)
    return bytes(body), False


def _raise_inference_error(response: Response) -> None:
    body, truncated = _read_limited_response(response, MAX_INFERENCE_ERROR_BYTES)
    code: str | None = None
    retryable: bool | None = None
    capacity: tuple[str, str, int] | None = None
    message = body.decode("utf-8", errors="replace").strip()
    if not truncated:
        try:
            payload = json.loads(body)
            if isinstance(payload, dict):
                code = payload.get("error") if isinstance(payload.get("error"), str) else None
                detail = payload.get("message")
                if isinstance(detail, str) and detail:
                    message = f"{detail} ({code})" if code and detail != code else detail
                elif code:
                    message = code
                if isinstance(payload.get("retryable"), bool):
                    retryable = payload["retryable"]
                reason = payload.get("reason")
                retry_after_ms = payload.get("retry_after_ms")
                if (
                    response.status_code == 503
                    and code is not None
                    and retryable is True
                    and reason in {"inference_capacity", "inference_admission", "request_queue"}
                    and type(retry_after_ms) is int
                    and retry_after_ms > 0
                ):
                    capacity = (code, reason, retry_after_ms)
        except (TypeError, ValueError):
            pass
    else:
        message = f"{response.reason_phrase or f'HTTP {response.status_code}'} (response body exceeded {MAX_INFERENCE_ERROR_BYTES} bytes)"
    if not message:
        message = response.reason_phrase or f"HTTP {response.status_code}"
    if capacity is not None:
        capacity_code, reason, retry_after_ms = capacity
        raise InferenceCapacityError(capacity_code, message, reason, retry_after_ms)
    raise InferenceAPIError(response.status_code, code, message, retryable)


def _iter_bounded_response_lines(response: Response, max_line_bytes: int) -> Iterator[bytes]:
    line = bytearray()
    for chunk in response.iter_bytes():
        offset = 0
        while offset < len(chunk):
            newline = chunk.find(b"\n", offset)
            end = len(chunk) if newline < 0 else newline
            piece = chunk[offset:end]
            if len(line) + len(piece) > max_line_bytes:
                raise AntflyException(f"generation SSE line exceeded {max_line_bytes} bytes")
            line.extend(piece)
            if newline < 0:
                break
            if line.endswith(b"\r"):
                del line[-1]
            yield bytes(line)
            line.clear()
            offset = newline + 1

    if line:
        if line.endswith(b"\r"):
            del line[-1]
        yield bytes(line)


def _decode_sse_value(value: bytes) -> str:
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise AntflyException("generation SSE stream contained invalid UTF-8") from exc


def _iter_sse_frames(response: Response) -> Iterator[tuple[str, str]]:
    event = ""
    data: list[str] = []
    data_bytes = 0

    for line in _iter_bounded_response_lines(response, MAX_GENERATION_SSE_LINE_BYTES):
        if line == b"":
            if data:
                yield event, "\n".join(data)
            event = ""
            data = []
            data_bytes = 0
            continue
        if line.startswith(b":"):
            continue

        field, separator, value = line.partition(b":")
        if separator and value.startswith(b" "):
            value = value[1:]
        if field == b"event":
            event = _decode_sse_value(value)
        elif field == b"data":
            data_bytes += (1 if data else 0) + len(value)
            if data_bytes > MAX_GENERATION_SSE_EVENT_BYTES:
                raise AntflyException(f"generation SSE event exceeded {MAX_GENERATION_SSE_EVENT_BYTES} bytes")
            data.append(_decode_sse_value(value))

    if data:
        yield event, "\n".join(data)


def _iter_generation_chunks(response: Response) -> Iterator[InferenceGenerateChunk]:
    for event, data in _iter_sse_frames(response):
        if event == "error":
            raise AntflyException(f"generation stream failed: {data}")
        if data.strip() == "[DONE]":
            return
        try:
            payload = json.loads(data)
            if not isinstance(payload, dict):
                raise ValueError("event data must be a JSON object")
            yield InferenceGenerateChunk.from_dict(payload)
        except (KeyError, TypeError, ValueError) as exc:
            raise AntflyException(f"generation stream returned invalid JSON: {exc}") from exc
    raise AntflyException("generation stream ended before [DONE]")


def normalize_base_url(base_url: str) -> str:
    """Return the Antfly server root URL for local or CloudAF endpoints."""
    trimmed = base_url.rstrip("/")
    if trimmed.endswith("/db/v1"):
        return trimmed[: -len("/db/v1")]
    if trimmed.endswith("/auth/v1"):
        return trimmed[: -len("/auth/v1")]
    if trimmed.endswith("/ai/v1"):
        return trimmed[: -len("/ai/v1")]
    return trimmed


class IndexOperations:
    """Resource-oriented index operations."""

    def __init__(self, client: "AntflyClient") -> None:
        self._client = client

    def create(
        self,
        table: str,
        name: str,
        config: CreateIndexRequest | Mapping[str, Any],
    ) -> CreatedIndex:
        """Create an index and return its typed normalized configuration.

        Generated request models provide type checking and editor completion.
        Mappings remain accepted for compatibility with existing callers.
        """
        payload = config.to_dict() if isinstance(config, _CREATE_INDEX_REQUEST_TYPES) else dict(config)
        if "name" in payload:
            raise ValueError("index name is owned by the path; pass it as the name argument")
        if not isinstance(payload.get("type"), str) or not payload["type"]:
            raise ValueError("index config requires a non-empty type")
        validate_create_index_request_relationships(payload)
        result = self._client._request(
            "POST",
            f"/db/v1/tables/{quote(table, safe='')}/indexes/{quote(name, safe='')}",
            json=payload,
        )
        if not isinstance(result, dict):
            raise AntflyException("create index returned an invalid response")
        try:
            discriminator = result.get("type")
            if not isinstance(discriminator, str):
                raise ValueError("missing string discriminator 'type'")
            response_type = _CREATED_INDEX_TYPES.get(discriminator)
            if response_type is None:
                raise ValueError(f"unsupported index type {discriminator!r}")
            return response_type.from_dict(result)
        except (KeyError, TypeError, ValueError) as exc:
            raise AntflyException(f"create index returned an invalid response: {exc}") from exc

    def list(self, table: str) -> list[dict[str, Any]]:
        """List all indexes on a table."""
        result = self._client._request("GET", f"/db/v1/tables/{quote(table, safe='')}/indexes")
        if not isinstance(result, list):
            raise AntflyException("list indexes returned an invalid response")
        return result

    def get(self, table: str, name: str) -> dict[str, Any]:
        """Get index configuration and readiness status."""
        result = self._client._request("GET", f"/db/v1/tables/{quote(table, safe='')}/indexes/{quote(name, safe='')}")
        if not isinstance(result, dict):
            raise AntflyException("get index returned an invalid response")
        return result

    def drop(self, table: str, name: str) -> None:
        """Drop an index."""
        self._client._request("DELETE", f"/db/v1/tables/{quote(table, safe='')}/indexes/{quote(name, safe='')}")


class AntflyClient:
    """High-level client for Antfly database and inference APIs."""

    def __init__(
        self,
        base_url: str,
        username: str | None = None,
        password: str | None = None,
        api_key: tuple[str, str] | None = None,
        token: str | None = None,
        timeout: float = 30.0,
        max_write_request_bytes: int = DEFAULT_WRITE_MAX_REQUEST_BYTES,
        max_json_response_bytes: int = DEFAULT_MAX_JSON_RESPONSE_BYTES,
        max_error_response_bytes: int = DEFAULT_MAX_ERROR_RESPONSE_BYTES,
    ):
        """
        Initialize Antfly client.

        Supports three authentication methods (mutually exclusive):
        - Basic auth: provide ``username`` and ``password``
        - API key: provide ``api_key`` as ``(key_id, key_secret)``
        - Token: provide ``token``

        Args:
            base_url: Base URL of the Antfly server or CloudAF proxy
            username: Username for basic authentication (optional)
            password: Password for basic authentication (optional)
            api_key: Tuple of (key_id, key_secret) for API key authentication (optional)
            token: Token string for token authentication (optional)
            timeout: Request timeout in seconds
            max_write_request_bytes: Maximum encoded JSON bytes for write requests
            max_json_response_bytes: Maximum bytes read for a successful JSON response
            max_error_response_bytes: Maximum bytes read from an error response
        """
        if max_json_response_bytes <= 0 or max_error_response_bytes <= 0:
            raise ValueError("response byte limits must be positive")
        self.base_url = normalize_base_url(base_url)
        self.max_write_request_bytes = max_write_request_bytes
        self.max_json_response_bytes = max_json_response_bytes
        self.max_error_response_bytes = max_error_response_bytes

        httpx_args: dict[str, Any] = {}

        if api_key is not None:
            key_id, key_secret = api_key
            encoded = base64.b64encode(f"{key_id}:{key_secret}".encode()).decode()
            self._client = AuthenticatedClient(
                base_url=self.base_url,
                token=encoded,
                prefix="ApiKey",
                timeout=Timeout(timeout),
                httpx_args=httpx_args,
            )
        elif token is not None:
            self._client = AuthenticatedClient(
                base_url=self.base_url,
                token=token,
                prefix="Bearer",
                timeout=Timeout(timeout),
                httpx_args=httpx_args,
            )
        else:
            if username and password:
                httpx_args["auth"] = (username, password)

            self._client = Client(
                base_url=self.base_url,
                timeout=Timeout(timeout),
                httpx_args=httpx_args,
            )
        self.indexes = IndexOperations(self)

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        """Make an HTTP request using the underlying httpx client.

        Args:
            method: HTTP method
            path: URL path (relative to base_url)
            **kwargs: Additional arguments passed to httpx

        Returns:
            Parsed JSON response

        Raises:
            AntflyException: If the request fails
        """
        with self._client.get_httpx_client().stream(method, path, **kwargs) as response:
            if response.status_code >= 400:
                body, truncated = _read_limited_response(response, self.max_error_response_bytes)
                if truncated:
                    msg = (
                        f"{response.reason_phrase or f'HTTP {response.status_code}'} "
                        f"(response body exceeded {self.max_error_response_bytes} bytes)"
                    )
                else:
                    text = body.decode("utf-8", errors="replace")
                    error_body: dict[str, Any] | None = None
                    try:
                        parsed_error = json.loads(body)
                        error_body = parsed_error if isinstance(parsed_error, dict) else None
                        error = error_body.get("error") if isinstance(error_body, dict) else None
                        msg = error if isinstance(error, str) else text
                    except (TypeError, ValueError):
                        msg = text
                    if not msg:
                        msg = response.reason_phrase or f"HTTP {response.status_code}"
                    if (
                        response.status_code == 429
                        and error_body is not None
                        and error_body.get("code") == "storage_resource_exhausted"
                        and error_body.get("retryable") is True
                    ):
                        retry_after_ms = error_body.get("retry_after_ms")
                        retry_after_ms = (
                            retry_after_ms
                            if isinstance(retry_after_ms, int)
                            and not isinstance(retry_after_ms, bool)
                            and retry_after_ms > 0
                            else 0
                        )
                        retry_after_header = response.headers.get("Retry-After")
                        try:
                            retry_after_seconds = int(retry_after_header) if retry_after_header else None
                        except ValueError:
                            retry_after_seconds = None
                        if retry_after_seconds is not None and retry_after_seconds <= 0:
                            retry_after_seconds = None
                        if retry_after_ms == 0 and retry_after_seconds is not None:
                            retry_after_ms = retry_after_seconds * 1000
                        detail = error_body.get("message")
                        raise StorageResourceExhaustedError(
                            detail if isinstance(detail, str) and detail else msg,
                            retry_after_ms,
                            retry_after_seconds,
                        )
                    if (
                        response.status_code == 503
                        and error_body is not None
                        and error_body.get("error") in INDEX_MUTATION_TEMPORARILY_UNAVAILABLE_CODES
                        and error_body.get("retryable") is True
                    ):
                        retry_after_header = response.headers.get("Retry-After")
                        try:
                            retry_after_seconds = int(retry_after_header) if retry_after_header else None
                        except ValueError:
                            retry_after_seconds = None
                        if retry_after_seconds is not None and retry_after_seconds <= 0:
                            retry_after_seconds = None
                        code = error_body["error"]
                        detail = error_body.get("message")
                        raise IndexMutationTemporarilyUnavailableError(
                            code,
                            detail if isinstance(detail, str) and detail else msg,
                            retry_after_seconds,
                        )
                raise AntflyException(f"Request failed ({response.status_code}): {msg}")
            if response.status_code == 204:
                return None

            body, truncated = _read_limited_response(response, self.max_json_response_bytes)
            if truncated:
                raise AntflyException(f"response exceeded {self.max_json_response_bytes} bytes")
            try:
                return json.loads(body)
            except (TypeError, ValueError) as exc:
                raise AntflyException("response returned invalid JSON") from exc

    def _encode_write_request(self, operation: str, body: dict[str, Any]) -> bytes:
        encoded = json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        if self.max_write_request_bytes <= 0:
            return encoded
        if len(encoded) > self.max_write_request_bytes:
            raise AntflyException(
                f"{operation} request encoded to {len(encoded)} bytes, exceeding max write request size "
                f"{self.max_write_request_bytes}"
            )
        return encoded

    def generate(self, request: InferenceGenerateRequest) -> InferenceGenerateResponse:
        """Generate one non-streaming chat completion."""
        body = request.to_dict()
        body["stream"] = False
        with self._client.get_httpx_client().stream(
            "POST",
            "/ai/v1/generate",
            json=body,
            headers={"Accept": "application/json"},
        ) as response:
            if response.status_code < 200 or response.status_code >= 300:
                _raise_inference_error(response)
            raw, truncated = _read_limited_response(response, MAX_GENERATION_RESPONSE_BYTES)
            if truncated:
                raise AntflyException(f"generation response exceeded {MAX_GENERATION_RESPONSE_BYTES} bytes")
            try:
                payload = json.loads(raw)
                if not isinstance(payload, dict):
                    raise ValueError("response must be a JSON object")
                return InferenceGenerateResponse.from_dict(payload)
            except (KeyError, TypeError, ValueError) as exc:
                raise AntflyException(f"generation returned invalid JSON: {exc}") from exc

    @contextmanager
    def generate_stream(self, request: InferenceGenerateRequest) -> Iterator[Iterator[InferenceGenerateChunk]]:
        """Open a context-managed SSE generation stream."""
        body = request.to_dict()
        body["stream"] = True
        with self._client.get_httpx_client().stream(
            "POST",
            "/ai/v1/generate",
            json=body,
            headers={"Accept": "text/event-stream"},
        ) as response:
            if response.status_code < 200 or response.status_code >= 300:
                _raise_inference_error(response)
            content_type = response.headers.get("content-type", "").partition(";")[0].strip().lower()
            if content_type != "text/event-stream":
                raise AntflyException(f"generation stream returned content type {content_type!r}")
            yield _iter_generation_chunks(response)

    # Table operations

    def create_table(
        self,
        name: str,
        num_shards: int | None = None,
        indexes: dict[str, Any] | None = None,
        schema: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Create a new table.

        Args:
            name: Name of the table
            num_shards: Number of shards for the table
            indexes: Index configurations
            schema: Table schema definition

        Returns:
            Created table object as a dictionary

        Raises:
            AntflyException: If table creation fails
        """
        body: dict[str, Any] = {}
        if num_shards is not None:
            body["num_shards"] = num_shards
        if indexes is not None:
            for index_name, config in indexes.items():
                try:
                    validate_create_index_request_relationships(config)
                except (TypeError, ValueError) as exc:
                    raise ValueError(f"invalid index {index_name!r}: {exc}") from exc
            body["indexes"] = indexes
        if schema is not None:
            body["schema"] = schema

        return self._request(
            "POST",
            f"/db/v1/tables/{quote(name, safe='')}",
            json=body,
        )

    def list_tables(self) -> list[dict[str, Any]]:
        """
        List all tables.

        Returns:
            List of table status objects

        Raises:
            AntflyException: If listing tables fails
        """
        return self._request("GET", "/db/v1/tables")

    def get_table(self, name: str) -> dict[str, Any]:
        """
        Get table details.

        Args:
            name: Name of the table

        Returns:
            Table status object as a dictionary

        Raises:
            AntflyException: If getting table fails
        """
        return self._request("GET", f"/db/v1/tables/{quote(name, safe='')}")

    def drop_table(self, name: str) -> None:
        """
        Drop a table.

        Args:
            name: Name of the table to drop

        Raises:
            AntflyException: If dropping table fails
        """
        self._request("DELETE", f"/db/v1/tables/{quote(name, safe='')}")

    def query(
        self,
        table: str,
        *,
        query: dict[str, Any] | None = None,
        full_text_search: dict[str, Any] | None = None,
        full_text_index: str | None = None,
        semantic_search: str | None = None,
        embedding_template: str | None = None,
        indexes: list[str] | None = None,
        filter_prefix: str | None = None,
        filter_query: dict[str, Any] | None = None,
        exclusion_query: dict[str, Any] | None = None,
        aggregations: dict[str, Any] | None = None,
        facets: dict[str, Any] | None = None,
        embeddings: dict[str, Any] | None = None,
        search_effort: float | None = None,
        fields: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        timeout_ms: int | None = None,
        order_by: list[dict[str, Any]] | None = None,
        search_after: list[Any] | None = None,
        search_before: list[Any] | None = None,
        distance_under: float | None = None,
        distance_over: float | None = None,
        merge_config: dict[str, Any] | None = None,
        count: bool | None = None,
        profile: bool | None = None,
        reranker: dict[str, Any] | None = None,
        analyses: dict[str, Any] | None = None,
        graph_queries: GraphQueriesInput | None = None,
        document_renderer: str | None = None,
        pruner: dict[str, Any] | None = None,
        join: dict[str, Any] | None = None,
        foreign_sources: dict[str, Any] | None = None,
    ) -> QueryResponses:
        """
        Query a table.

        Args:
            table: Table name
            query: Canonical public query AST
            full_text_search: Full-text query object
            full_text_index: Named full-text index used by the scoring text query
            semantic_search: Natural-language vector search query
            embedding_template: Optional multimodal embedding template
            indexes: Vector index names for semantic search
            filter_prefix: Key prefix filter
            filter_query: Structured or full-text filter query
            exclusion_query: Structured or full-text exclusion query
            aggregations: Aggregation requests keyed by aggregation name
            facets: Backwards-compatible alias for ``aggregations``
            embeddings: Pre-computed embeddings keyed by index name
            search_effort: Vector recall/latency tradeoff
            fields: Source fields to include
            limit: Maximum number of hits
            offset: Offset for shallow pagination
            timeout_ms: Query deadline in milliseconds
            order_by: Exact sort fields. Returned hit ``_sort`` values can be
                passed back through ``search_after`` or ``search_before``.
            search_after: Forward cursor tuple copied from the previous page's
                last hit ``_sort`` value.
            search_before: Backward cursor tuple copied from the current page's
                first hit ``_sort`` value.
            distance_under: Maximum semantic distance
            distance_over: Minimum semantic distance
            merge_config: Hybrid merge configuration
            count: Return only total count when true
            profile: Include execution profile when true
            reranker: Reranker configuration
            analyses: Analysis configuration
            graph_queries: Named canonical graph operations. Accepts generated graph query models,
                ``GraphQueries``, or raw mappings.
            document_renderer: Handlebars document renderer
            pruner: Result pruning configuration
            join: Join configuration
            foreign_sources: Query-time foreign source configuration

        Returns:
            Generated ``QueryResponses`` model.

        Raises:
            AntflyException: If the query fails or both ``aggregations`` and
                ``facets`` are supplied.
        """
        if aggregations is not None and facets is not None:
            raise AntflyException("query accepts either aggregations or facets, not both")
        encoded_graph_queries = _serialize_graph_queries(graph_queries) if graph_queries is not None else None

        body: dict[str, Any] = {}

        query_fields = {
            "query": query,
            "full_text_search": full_text_search,
            "full_text_index": full_text_index,
            "semantic_search": semantic_search,
            "embedding_template": embedding_template,
            "indexes": indexes,
            "filter_prefix": filter_prefix,
            "filter_query": filter_query,
            "exclusion_query": exclusion_query,
            "aggregations": aggregations if aggregations is not None else facets,
            "embeddings": embeddings,
            "search_effort": search_effort,
            "fields": fields,
            "limit": limit,
            "offset": offset,
            "timeout_ms": timeout_ms,
            "order_by": order_by,
            "search_after": search_after,
            "search_before": search_before,
            "distance_under": distance_under,
            "distance_over": distance_over,
            "merge_config": merge_config,
            "count": count,
            "profile": profile,
            "reranker": reranker,
            "analyses": analyses,
            "graph_queries": encoded_graph_queries,
            "document_renderer": document_renderer,
            "pruner": pruner,
            "join": join,
            "foreign_sources": foreign_sources,
        }
        for key, value in query_fields.items():
            if value is not None:
                body[key] = value

        response = self._request("POST", f"/db/v1/tables/{quote(table, safe='')}/query", json=body)
        expected_graph_queries = None
        if encoded_graph_queries is not None:
            graph_dialect = "canonical"
            expected_graph_operations = None
            expected_graph_queries = encoded_graph_queries
        else:
            graph_dialect = "none"
            # Absence of graph_queries means graph validation is disabled, not
            # that a graph result envelope with zero operations is required.
            expected_graph_operations = None
        return decode_query_responses(
            response,
            graph_dialect=graph_dialect,
            expected_graph_operations=expected_graph_operations,
            expected_graph_queries=expected_graph_queries,
            query_table=table,
        )

    def get(self, table: str, key: str) -> dict[str, Any]:
        """
        Get a single record by key.

        Args:
            table: Table name
            key: Record key

        Returns:
            Record data

        Raises:
            AntflyException: If lookup fails
        """
        response = lookup_key.sync(
            table_name=table,
            key=key,
            client=cast(AuthenticatedClient, self._client),
        )

        if isinstance(response, Error):
            raise AntflyException(f"Failed to get key '{key}' from table '{table}': {response.error}")
        if response is None:
            raise AntflyException(f"Failed to get key '{key}' from table '{table}'")

        return response.to_dict()

    def list_artifact_enrichments(self, table: str) -> dict[str, Any]:
        """
        List table-level generated artifact enrichments.

        Args:
            table: Table name

        Returns:
            Artifact enrichment list response

        Raises:
            AntflyException: If listing artifact enrichments fails
        """
        return self._request("GET", f"/db/v1/tables/{quote(table, safe='')}/artifacts")

    def put_artifact_enrichment(
        self,
        table: str,
        artifact: str,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Register or replace a table-level generated artifact enrichment.

        Args:
            table: Table name
            artifact: Artifact enrichment name
            config: Enrichment configuration

        Returns:
            API response body

        Raises:
            AntflyException: If storing the artifact enrichment fails
        """
        return self._request(
            "PUT",
            f"/db/v1/tables/{quote(table, safe='')}/artifacts/{quote(artifact, safe='')}/enrichment",
            json=config,
        )

    def delete_artifact_enrichment(self, table: str, artifact: str) -> dict[str, Any]:
        """
        Delete a table-level generated artifact enrichment.

        Args:
            table: Table name
            artifact: Artifact enrichment name

        Returns:
            API response body

        Raises:
            AntflyException: If deleting the artifact enrichment fails
        """
        return self._request(
            "DELETE",
            f"/db/v1/tables/{quote(table, safe='')}/artifacts/{quote(artifact, safe='')}/enrichment",
        )

    def list_document_artifacts(
        self,
        table: str,
        key: str,
        detail: str = "summary",
    ) -> dict[str, Any]:
        """
        List generated artifact manifests attached to a document.

        Args:
            table: Table name
            key: Document key
            detail: Response detail level, ``summary`` or ``raw``

        Returns:
            Document artifact manifest list

        Raises:
            AntflyException: If listing document artifacts fails
        """
        return self._request(
            "GET",
            f"/db/v1/tables/{quote(table, safe='')}/documents/{quote(key, safe='')}/artifacts",
            params={"detail": detail},
        )

    def get_document_artifact(
        self,
        table: str,
        key: str,
        artifact: str,
        detail: str = "raw",
    ) -> dict[str, Any]:
        """
        Get a generated artifact manifest attached to a document.

        Args:
            table: Table name
            key: Document key
            artifact: Artifact name
            detail: Response detail level, ``summary`` or ``raw``

        Returns:
            Document artifact manifest

        Raises:
            AntflyException: If getting the document artifact fails
        """
        return self._request(
            "GET",
            f"/db/v1/tables/{quote(table, safe='')}/documents/{quote(key, safe='')}/artifacts/{quote(artifact, safe='')}",
            params={"detail": detail},
        )

    def reprocess_document_artifact(self, table: str, key: str, artifact: str) -> dict[str, Any]:
        """
        Reprocess one generated artifact for one document.

        Args:
            table: Table name
            key: Document key
            artifact: Artifact name

        Returns:
            Reprocess response

        Raises:
            AntflyException: If reprocessing fails
        """
        return self._request(
            "POST",
            f"/db/v1/tables/{quote(table, safe='')}/documents/{quote(key, safe='')}/artifacts/{quote(artifact, safe='')}/reprocess",
        )

    def reprocess_artifact_range(
        self,
        table: str,
        artifact: str,
        request: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Run one bounded table-wide reprocess pass for an artifact.

        Args:
            table: Table name
            artifact: Artifact name
            request: Optional reprocess request body

        Returns:
            Reprocess response

        Raises:
            AntflyException: If reprocessing fails
        """
        return self._request(
            "POST",
            f"/db/v1/tables/{quote(table, safe='')}/artifacts/{quote(artifact, safe='')}/reprocess",
            json=request or {},
        )

    def start_artifact_reprocess_job(
        self,
        table: str,
        artifact: str,
        request: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Start a durable table-wide artifact reprocess job.

        Args:
            table: Table name
            artifact: Artifact name
            request: Optional job start request body

        Returns:
            Reprocess job state

        Raises:
            AntflyException: If starting the job fails
        """
        return self._request(
            "POST",
            f"/db/v1/tables/{quote(table, safe='')}/artifacts/{quote(artifact, safe='')}/reprocess-jobs",
            json=request or {},
        )

    def get_artifact_reprocess_job(self, table: str, artifact: str, job_id: str) -> dict[str, Any]:
        """
        Get a durable artifact reprocess job.

        Args:
            table: Table name
            artifact: Artifact name
            job_id: Job identifier

        Returns:
            Reprocess job state

        Raises:
            AntflyException: If loading the job fails
        """
        return self._request(
            "GET",
            f"/db/v1/tables/{quote(table, safe='')}/artifacts/{quote(artifact, safe='')}/reprocess-jobs/{quote(job_id, safe='')}",
        )

    def advance_artifact_reprocess_job(self, table: str, artifact: str, job_id: str) -> dict[str, Any]:
        """
        Advance a durable artifact reprocess job.

        Args:
            table: Table name
            artifact: Artifact name
            job_id: Job identifier

        Returns:
            Reprocess job state

        Raises:
            AntflyException: If advancing the job fails
        """
        return self._request(
            "POST",
            f"/db/v1/tables/{quote(table, safe='')}/artifacts/{quote(artifact, safe='')}/reprocess-jobs/{quote(job_id, safe='')}/advance",
        )

    def cancel_artifact_reprocess_job(self, table: str, artifact: str, job_id: str) -> dict[str, Any]:
        """
        Cancel a durable artifact reprocess job.

        Args:
            table: Table name
            artifact: Artifact name
            job_id: Job identifier

        Returns:
            Reprocess job state

        Raises:
            AntflyException: If canceling the job fails
        """
        return self._request(
            "POST",
            f"/db/v1/tables/{quote(table, safe='')}/artifacts/{quote(artifact, safe='')}/reprocess-jobs/{quote(job_id, safe='')}/cancel",
        )

    def batch(
        self,
        table: str,
        inserts: dict[str, dict[str, Any]] | None = None,
        deletes: list[str] | None = None,
    ) -> None:
        """
        Perform batch operations on a table.

        Args:
            table: Table name
            inserts: Dictionary of key-value pairs to insert
            deletes: List of keys to delete

        Raises:
            AntflyException: If batch operation fails
        """
        batch_inserts = BatchRequestInserts.from_dict(inserts) if inserts is not None else UNSET
        request = BatchRequest(inserts=batch_inserts, deletes=deletes or [])
        encoded = self._encode_write_request("Batch", request.to_dict())

        self._request(
            "POST",
            f"/db/v1/tables/{quote(table, safe='')}/batch",
            content=encoded,
            headers={"Content-Type": "application/json"},
        )
