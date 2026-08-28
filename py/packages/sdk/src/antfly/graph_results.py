"""Fail-closed decoding for canonical graph query results.

The generated client owns wire models.  Cross-field invariants and strict
object-shape validation remain handwritten here so generator upgrades cannot
silently weaken the public graph result contract.
"""

from __future__ import annotations

from collections.abc import Mapping
from math import isfinite
from typing import Any, Literal, NamedTuple, NoReturn

from .client_generated.models.query_responses import QueryResponses
from .exceptions import AntflyException
from .graph_identifier_policy_generated import is_valid_graph_identifier

_MAX_GRAPH_ALIASES = 64
_MAX_GRAPH_EDGES = 64
_MAX_GRAPH_ITEMS = 10_000
_MISSING = object()
GraphResultDialect = Literal["auto", "canonical", "legacy", "none"]
GraphResultKind = Literal["bindings", "aggregates", "nodes"]
GraphNodeResultMode = Literal["traversal", "shortest_path", "k_shortest_paths"]


class _CanonicalResultContract(NamedTuple):
    kind: GraphResultKind
    names: frozenset[str] | None = None
    max_items: int | None = None
    node_mode: GraphNodeResultMode | None = None
    include_paths: bool = False
    include_documents: bool = False


def _invalid(path: str, message: str) -> NoReturn:
    raise AntflyException(f"query returned invalid graph response at {path}: {message}")


def _object(value: object, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _invalid(path, "must be an object with string keys")
    return value


def _array(value: object, path: str) -> list[Any]:
    if not isinstance(value, list):
        _invalid(path, "must be an array")
    return value


def _exact_keys(
    value: Mapping[str, Any],
    path: str,
    *,
    required: frozenset[str],
    optional: frozenset[str] = frozenset(),
) -> None:
    keys = frozenset(value)
    missing = required - keys
    if missing:
        _invalid(path, f"is missing required member {min(missing)!r}")
    unexpected = keys - required - optional
    if unexpected:
        _invalid(path, f"contains unknown member {min(unexpected)!r}")


def _nonempty_string(value: object, path: str, *, max_utf8_bytes: int | None = None) -> str:
    if not isinstance(value, str) or not value:
        _invalid(path, "must be a non-empty string")
    if max_utf8_bytes is not None:
        try:
            encoded_length = len(value.encode("utf-8"))
        except UnicodeEncodeError:
            _invalid(path, "must contain valid UTF-8")
        if encoded_length > max_utf8_bytes:
            _invalid(path, f"must encode to at most {max_utf8_bytes} UTF-8 bytes")
    return value


def _bounded_integer(value: object, path: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or value < minimum or value > maximum:
        _invalid(path, f"must be an integer between {minimum} and {maximum}")
    return value


def _finite_nonnegative(value: object, path: str, *, at_most_one: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _invalid(path, "must be a finite non-negative number")
    try:
        number = float(value)
    except (OverflowError, ValueError):
        _invalid(path, "must be a finite non-negative number")
    if not isfinite(number) or number < 0 or (at_most_one and number > 1):
        _invalid(path, "must be a finite number in [0,1]" if at_most_one else "must be a finite non-negative number")
    return number


def _finite_number(value: object, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _invalid(path, "must be a finite number")
    try:
        number = float(value)
    except (OverflowError, ValueError):
        _invalid(path, "must be a finite number")
    if not isfinite(number):
        _invalid(path, "must be a finite number")
    return number


def _same_endpoint(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return left["key"] == right["key"] and left.get("table", _MISSING) == right.get("table", _MISSING)


def _validate_endpoint(value: object, path: str) -> Mapping[str, Any]:
    endpoint = _object(value, path)
    _exact_keys(endpoint, path, required=frozenset({"key"}), optional=frozenset({"table"}))
    _nonempty_string(endpoint["key"], f"{path}.key")
    if "table" in endpoint:
        _nonempty_string(endpoint["table"], f"{path}.table")
    return endpoint


def _validate_path_edge(
    value: object,
    path: str,
    expected_from: Mapping[str, Any],
    expected_to: Mapping[str, Any],
    *,
    max_weight_mode: bool,
) -> float:
    edge = _object(value, path)
    _exact_keys(
        edge,
        path,
        required=frozenset({"from", "to", "direction", "type", "weight"}),
        optional=frozenset({"metadata"}),
    )
    from_endpoint = _validate_endpoint(edge["from"], f"{path}.from")
    to_endpoint = _validate_endpoint(edge["to"], f"{path}.to")
    if not _same_endpoint(from_endpoint, expected_from) or not _same_endpoint(to_endpoint, expected_to):
        _invalid(path, "endpoints do not match adjacent path nodes")
    if edge["direction"] not in {"out", "in"}:
        _invalid(f"{path}.direction", "must be out or in")
    _nonempty_string(edge["type"], f"{path}.type", max_utf8_bytes=65_536)
    weight = _finite_nonnegative(edge["weight"], f"{path}.weight", at_most_one=max_weight_mode)
    if "metadata" in edge:
        _object(edge["metadata"], f"{path}.metadata")
    return weight


def _float_equal(left: float, right: float) -> bool:
    return abs(left - right) <= 1e-12 * max(1.0, abs(left), abs(right))


def _validate_path(value: object, path: str) -> Mapping[str, Any]:
    graph_path = _object(value, path)
    _exact_keys(
        graph_path,
        path,
        required=frozenset({"nodes", "edges", "length", "weight_mode", "weight_sum", "objective_value"}),
    )
    raw_nodes = _array(graph_path["nodes"], f"{path}.nodes")
    raw_edges = _array(graph_path["edges"], f"{path}.edges")
    if not 1 <= len(raw_nodes) <= _MAX_GRAPH_EDGES + 1:
        _invalid(f"{path}.nodes", f"must contain between 1 and {_MAX_GRAPH_EDGES + 1} items")
    if len(raw_edges) > _MAX_GRAPH_EDGES:
        _invalid(f"{path}.edges", f"must contain at most {_MAX_GRAPH_EDGES} items")
    length = _bounded_integer(graph_path["length"], f"{path}.length", 0, _MAX_GRAPH_EDGES)
    if length != len(raw_edges) or len(raw_nodes) != len(raw_edges) + 1:
        _invalid(path, "length, nodes, and edges do not align")
    nodes = [_validate_endpoint(node, f"{path}.nodes[{index}]") for index, node in enumerate(raw_nodes)]
    weight_mode = graph_path["weight_mode"]
    if weight_mode not in {"min_hops", "min_weight", "max_weight"}:
        _invalid(f"{path}.weight_mode", "has an unknown value")

    weight_sum = 0.0
    weight_product = 1.0
    for index, edge in enumerate(raw_edges):
        weight = _validate_path_edge(
            edge,
            f"{path}.edges[{index}]",
            nodes[index],
            nodes[index + 1],
            max_weight_mode=weight_mode == "max_weight",
        )
        weight_sum += weight
        weight_product *= weight
        if not isfinite(weight_sum) or not isfinite(weight_product):
            _invalid(path, "path score overflowed")

    encoded_sum = _finite_nonnegative(graph_path["weight_sum"], f"{path}.weight_sum")
    encoded_objective = _finite_nonnegative(graph_path["objective_value"], f"{path}.objective_value")
    if not _float_equal(encoded_sum, weight_sum):
        _invalid(f"{path}.weight_sum", "does not equal the sum of edge weights")
    objective = (
        float(length) if weight_mode == "min_hops" else weight_product if weight_mode == "max_weight" else weight_sum
    )
    if not _float_equal(encoded_objective, objective):
        _invalid(f"{path}.objective_value", "does not match weight_mode")
    return graph_path


def _validate_result_node(value: object, path: str) -> Mapping[str, Any]:
    node = _object(value, path)
    _exact_keys(
        node,
        path,
        required=frozenset({"key", "depth"}),
        optional=frozenset({"table", "document", "path", "path_edges", "provenance", "evidence"}),
    )
    _nonempty_string(node["key"], f"{path}.key")
    _bounded_integer(node["depth"], f"{path}.depth", 0, _MAX_GRAPH_EDGES)
    if "table" in node:
        _nonempty_string(node["table"], f"{path}.table")
    if "document" in node:
        _object(node["document"], f"{path}.document")
    if "provenance" in node:
        provenance = _array(node["provenance"], f"{path}.provenance")
        for index, label in enumerate(provenance):
            if not isinstance(label, str):
                _invalid(f"{path}.provenance[{index}]", "must be a string")
    if "evidence" in node:
        _object(node["evidence"], f"{path}.evidence")

    endpoints: list[Mapping[str, Any]] | None = None
    if "path" in node:
        raw_path = _array(node["path"], f"{path}.path")
        if not 1 <= len(raw_path) <= _MAX_GRAPH_EDGES + 1:
            _invalid(f"{path}.path", f"must contain between 1 and {_MAX_GRAPH_EDGES + 1} items")
        if node["depth"] != len(raw_path) - 1:
            _invalid(f"{path}.depth", "must equal path length minus one")
        endpoints = [_validate_endpoint(endpoint, f"{path}.path[{index}]") for index, endpoint in enumerate(raw_path)]
        if not _same_endpoint(endpoints[-1], node):
            _invalid(f"{path}.path", "must terminate at the result node")
    if "path_edges" in node:
        raw_edges = _array(node["path_edges"], f"{path}.path_edges")
        if endpoints is None or len(raw_edges) + 1 != len(endpoints):
            _invalid(f"{path}.path_edges", "must align with path")
        for index, edge in enumerate(raw_edges):
            _validate_path_edge(
                edge,
                f"{path}.path_edges[{index}]",
                endpoints[index],
                endpoints[index + 1],
                max_weight_mode=False,
            )
    return node


def _validate_stats(value: object, path: str, expected_items: int, *, allow_truncated: bool) -> None:
    stats = _object(value, path)
    _exact_keys(stats, path, required=frozenset({"returned_items", "truncated"}))
    returned_items = _bounded_integer(stats["returned_items"], f"{path}.returned_items", 0, _MAX_GRAPH_ITEMS)
    if returned_items != expected_items:
        _invalid(f"{path}.returned_items", "does not match the result payload")
    if type(stats["truncated"]) is not bool:
        _invalid(f"{path}.truncated", "must be a boolean")
    if not allow_truncated and stats["truncated"]:
        _invalid(f"{path}.truncated", "must be false for exact aggregates")


def _validate_bindings_result(value: Mapping[str, Any], path: str) -> None:
    _exact_keys(value, path, required=frozenset({"kind", "rows", "stats"}))
    rows = _array(value["rows"], f"{path}.rows")
    if len(rows) > _MAX_GRAPH_ITEMS:
        _invalid(f"{path}.rows", f"must contain at most {_MAX_GRAPH_ITEMS} items")
    for row_index, raw_row in enumerate(rows):
        row_path = f"{path}.rows[{row_index}]"
        row = _object(raw_row, row_path)
        if not 1 <= len(row) <= _MAX_GRAPH_ALIASES:
            _invalid(row_path, f"must contain between 1 and {_MAX_GRAPH_ALIASES} bindings")
        for alias, raw_binding in row.items():
            if not is_valid_graph_identifier(alias):
                _invalid(row_path, f"contains invalid graph alias {alias!r}")
            if raw_binding is None:
                continue
            binding_path = f"{row_path}.{alias}"
            binding = _object(raw_binding, binding_path)
            _exact_keys(
                binding,
                binding_path,
                required=frozenset({"key"}),
                optional=frozenset({"table", "document"}),
            )
            _nonempty_string(binding["key"], f"{binding_path}.key")
            if "table" in binding:
                _nonempty_string(binding["table"], f"{binding_path}.table")
            if "document" in binding:
                _object(binding["document"], f"{binding_path}.document")
    _validate_stats(value["stats"], f"{path}.stats", len(rows), allow_truncated=True)


def _validate_aggregates_result(value: Mapping[str, Any], path: str) -> None:
    _exact_keys(value, path, required=frozenset({"kind", "aggregates", "stats"}))
    aggregates = _object(value["aggregates"], f"{path}.aggregates")
    if not 1 <= len(aggregates) <= _MAX_GRAPH_ALIASES:
        _invalid(f"{path}.aggregates", f"must contain between 1 and {_MAX_GRAPH_ALIASES} values")
    for name, raw_aggregate in aggregates.items():
        if not is_valid_graph_identifier(name):
            _invalid(f"{path}.aggregates", f"contains invalid aggregate name {name!r}")
        aggregate_path = f"{path}.aggregates.{name}"
        aggregate = _object(raw_aggregate, aggregate_path)
        _exact_keys(aggregate, aggregate_path, required=frozenset({"value", "exact"}))
        decimal = aggregate["value"]
        if not isinstance(decimal, str) or not decimal or not decimal.isascii() or not decimal.isdigit():
            _invalid(f"{aggregate_path}.value", "must be an unsigned decimal string")
        if aggregate["exact"] is not True:
            _invalid(f"{aggregate_path}.exact", "must be true")
    _validate_stats(value["stats"], f"{path}.stats", len(aggregates), allow_truncated=False)


def _validate_nodes_result(value: Mapping[str, Any], path: str) -> None:
    _exact_keys(value, path, required=frozenset({"kind", "nodes", "paths", "stats"}))
    raw_nodes = _array(value["nodes"], f"{path}.nodes")
    raw_paths = _array(value["paths"], f"{path}.paths")
    if len(raw_nodes) > _MAX_GRAPH_ITEMS or len(raw_paths) > _MAX_GRAPH_ITEMS:
        _invalid(path, f"nodes and paths must each contain at most {_MAX_GRAPH_ITEMS} items")
    nodes = [_validate_result_node(node, f"{path}.nodes[{index}]") for index, node in enumerate(raw_nodes)]
    paths = [_validate_path(graph_path, f"{path}.paths[{index}]") for index, graph_path in enumerate(raw_paths)]
    if paths:
        if len(nodes) != len(paths):
            _invalid(path, "path results require one terminal node per path")
        for index, graph_path in enumerate(paths):
            if not _same_endpoint(graph_path["nodes"][-1], nodes[index]):
                _invalid(f"{path}.nodes[{index}]", "does not match its path terminal")
    _validate_stats(value["stats"], f"{path}.stats", len(paths) if paths else len(nodes), allow_truncated=True)


def _legacy_integer(value: object, path: str) -> int:
    if type(value) is not int:
        _invalid(path, "must be an integer")
    return value


def _validate_legacy_path_edge(value: object, path: str) -> None:
    edge = _object(value, path)
    for name in ("source", "target", "type"):
        if name in edge and not isinstance(edge[name], str):
            _invalid(f"{path}.{name}", "must be a string")
    if "weight" in edge:
        _finite_number(edge["weight"], f"{path}.weight")
    if "metadata" in edge:
        _object(edge["metadata"], f"{path}.metadata")


def _validate_legacy_path(value: object, path: str) -> None:
    graph_path = _object(value, path)
    if "nodes" in graph_path:
        for index, node in enumerate(_array(graph_path["nodes"], f"{path}.nodes")):
            if not isinstance(node, str):
                _invalid(f"{path}.nodes[{index}]", "must be a string")
    if "edges" in graph_path:
        for index, edge in enumerate(_array(graph_path["edges"], f"{path}.edges")):
            _validate_legacy_path_edge(edge, f"{path}.edges[{index}]")
    if "total_weight" in graph_path:
        _finite_nonnegative(graph_path["total_weight"], f"{path}.total_weight")
    if "length" in graph_path:
        _legacy_integer(graph_path["length"], f"{path}.length")


def _validate_legacy_node(value: object, path: str) -> None:
    node = _object(value, path)
    if "key" not in node or not isinstance(node["key"], str):
        _invalid(f"{path}.key", "must be a string")
    if "table" in node and not isinstance(node["table"], str):
        _invalid(f"{path}.table", "must be a string")
    if "depth" in node:
        _legacy_integer(node["depth"], f"{path}.depth")
    if "distance" in node:
        _finite_number(node["distance"], f"{path}.distance")
    if "document" in node:
        _object(node["document"], f"{path}.document")
    if "evidence" in node:
        _object(node["evidence"], f"{path}.evidence")
    for name in ("path", "provenance"):
        if name in node:
            for index, item in enumerate(_array(node[name], f"{path}.{name}")):
                if not isinstance(item, str):
                    _invalid(f"{path}.{name}[{index}]", "must be a string")
    if "path_edges" in node:
        for index, edge in enumerate(_array(node["path_edges"], f"{path}.path_edges")):
            _validate_legacy_path_edge(edge, f"{path}.path_edges[{index}]")
    if "edges" in node:
        _array(node["edges"], f"{path}.edges")


def _validate_legacy_result(value: Mapping[str, Any], path: str) -> None:
    _exact_keys(
        value,
        path,
        required=frozenset({"type", "total"}),
        optional=frozenset({"kind", "nodes", "paths", "matches", "took"}),
    )
    if "kind" in value and value["kind"] != "legacy":
        _invalid(f"{path}.kind", "must be 'legacy' when present")
    if not isinstance(value["type"], str) or value["type"] not in {
        "neighbors",
        "traverse",
        "shortest_path",
        "k_shortest_paths",
        "pattern",
    }:
        _invalid(f"{path}.type", "has an unknown legacy graph query type")
    _legacy_integer(value["total"], f"{path}.total")
    if "took" in value:
        _legacy_integer(value["took"], f"{path}.took")
    if "nodes" in value:
        for index, node in enumerate(_array(value["nodes"], f"{path}.nodes")):
            _validate_legacy_node(node, f"{path}.nodes[{index}]")
    if "paths" in value:
        for index, graph_path in enumerate(_array(value["paths"], f"{path}.paths")):
            _validate_legacy_path(graph_path, f"{path}.paths[{index}]")
    if "matches" in value:
        for index, raw_match in enumerate(_array(value["matches"], f"{path}.matches")):
            match_path = f"{path}.matches[{index}]"
            match = _object(raw_match, match_path)
            if "bindings" in match:
                for name, binding in _object(match["bindings"], f"{match_path}.bindings").items():
                    _validate_legacy_node(binding, f"{match_path}.bindings.{name}")
            if "path" in match:
                for edge_index, edge in enumerate(_array(match["path"], f"{match_path}.path")):
                    _validate_legacy_path_edge(edge, f"{match_path}.path[{edge_index}]")


def _canonical_result_contract(value: object, path: str) -> _CanonicalResultContract:
    operation = _object(value, path)
    if "match" in operation:
        returned = _object(operation.get("return", _MISSING), f"{path}.return")
        if "bindings" in returned:
            bindings = _array(returned["bindings"], f"{path}.return.bindings")
            names: list[str] = []
            for index, name in enumerate(bindings):
                if not isinstance(name, str):
                    _invalid(f"{path}.return.bindings[{index}]", "must be a string")
                names.append(name)
            raw_limit = returned.get("limit", 100)
            limit = _bounded_integer(raw_limit, f"{path}.return.limit", 1, _MAX_GRAPH_ITEMS)
            return _CanonicalResultContract(
                "bindings",
                frozenset(names),
                limit,
                include_documents=returned.get("include_documents") is True,
            )
        if "aggregates" in returned:
            aggregates = _object(returned["aggregates"], f"{path}.return.aggregates")
            return _CanonicalResultContract("aggregates", frozenset(aggregates))
        _invalid(f"{path}.return", "must select bindings or aggregates")
    if "traverse" in operation:
        traversal = _object(operation["traverse"], f"{path}.traverse")
        limit = _bounded_integer(traversal.get("limit", 100), f"{path}.traverse.limit", 1, _MAX_GRAPH_ITEMS)
        return _CanonicalResultContract(
            "nodes",
            max_items=limit,
            node_mode="traversal",
            include_paths=traversal.get("include_paths") is True,
            include_documents=traversal.get("include_documents") is True,
        )
    if "shortest_path" in operation:
        shortest_path = _object(operation["shortest_path"], f"{path}.shortest_path")
        return _CanonicalResultContract(
            "nodes",
            max_items=1,
            node_mode="shortest_path",
            include_documents=shortest_path.get("include_documents") is True,
        )
    if "k_shortest_paths" in operation:
        k_shortest_paths = _object(operation["k_shortest_paths"], f"{path}.k_shortest_paths")
        k = _bounded_integer(k_shortest_paths.get("k", _MISSING), f"{path}.k_shortest_paths.k", 1, 100)
        return _CanonicalResultContract(
            "nodes",
            max_items=k,
            node_mode="k_shortest_paths",
            include_documents=k_shortest_paths.get("include_documents") is True,
        )
    _invalid(path, "does not contain a supported graph operation")


def _validate_graph_result(
    value: object,
    path: str,
    dialect: GraphResultDialect,
    contract: _CanonicalResultContract | None = None,
) -> None:
    result = _object(value, path)
    kind = result.get("kind", _MISSING)
    if dialect == "none":
        _invalid(path, "was returned for a request without graph operations")
    if kind is _MISSING or kind == "legacy":
        if dialect == "canonical":
            _invalid(f"{path}.kind", "canonical graph results require a discriminator")
        _validate_legacy_result(result, path)
        return
    if dialect == "legacy":
        _invalid(f"{path}.kind", "legacy graph results must use the legacy result shape")
    if not isinstance(kind, str):
        _invalid(f"{path}.kind", "must be a string")
    if contract is not None and kind != contract.kind:
        _invalid(f"{path}.kind", f"must be {contract.kind!r} for the requested operation")
    if kind == "bindings":
        _validate_bindings_result(result, path)
        if contract is not None and contract.max_items is not None:
            rows = _array(result["rows"], f"{path}.rows")
            if len(rows) > contract.max_items:
                _invalid(f"{path}.rows", "exceeds the requested limit")
        if contract is not None and contract.names is not None:
            expected = contract.names
            for row_index, raw_row in enumerate(_array(result["rows"], f"{path}.rows")):
                row = _object(raw_row, f"{path}.rows[{row_index}]")
                if frozenset(row) != expected:
                    _invalid(f"{path}.rows[{row_index}]", "binding aliases do not match the requested projection")
                if not contract.include_documents:
                    for alias, raw_binding in row.items():
                        if raw_binding is not None and "document" in _object(
                            raw_binding, f"{path}.rows[{row_index}].{alias}"
                        ):
                            _invalid(
                                f"{path}.rows[{row_index}].{alias}.document",
                                "was returned without being requested",
                            )
    elif kind == "aggregates":
        _validate_aggregates_result(result, path)
        if contract is not None and contract.names is not None:
            aggregates = _object(result["aggregates"], f"{path}.aggregates")
            if frozenset(aggregates) != contract.names:
                _invalid(f"{path}.aggregates", "names do not match the requested aggregates")
    elif kind == "nodes":
        _validate_nodes_result(result, path)
        if contract is not None:
            raw_nodes = _array(result["nodes"], f"{path}.nodes")
            raw_paths = _array(result["paths"], f"{path}.paths")
            if contract.max_items is None or contract.node_mode is None:
                _invalid(path, "has no node operation contract")
            if len(raw_nodes) > contract.max_items or len(raw_paths) > contract.max_items:
                _invalid(path, "exceeds the requested result limit")
            if not contract.include_documents:
                for index, raw_node in enumerate(raw_nodes):
                    if "document" in _object(raw_node, f"{path}.nodes[{index}]"):
                        _invalid(f"{path}.nodes[{index}].document", "was returned without being requested")
            if contract.node_mode == "traversal":
                if raw_paths:
                    _invalid(f"{path}.paths", "traversal paths belong on result nodes")
                for index, raw_node in enumerate(raw_nodes):
                    node = _object(raw_node, f"{path}.nodes[{index}]")
                    if contract.include_paths:
                        if "path" not in node:
                            _invalid(f"{path}.nodes[{index}]", "is missing its requested path")
                    else:
                        if "path" in node or "path_edges" in node:
                            _invalid(f"{path}.nodes[{index}]", "contains a path that was not requested")
            else:
                if len(raw_nodes) != len(raw_paths):
                    _invalid(path, "path results require one terminal node per path")
                stats = _object(result["stats"], f"{path}.stats")
                if stats["truncated"]:
                    _invalid(f"{path}.stats.truncated", "must be false for an exact path result")
                for index, (raw_node, raw_path) in enumerate(zip(raw_nodes, raw_paths, strict=True)):
                    node = _object(raw_node, f"{path}.nodes[{index}]")
                    graph_path = _object(raw_path, f"{path}.paths[{index}]")
                    if "path" in node or "path_edges" in node:
                        _invalid(
                            f"{path}.nodes[{index}]",
                            "duplicates its authoritative top-level path",
                        )
                    if node["depth"] != graph_path["length"]:
                        _invalid(f"{path}.nodes[{index}].depth", "does not match its path length")
    else:
        _invalid(f"{path}.kind", f"has unknown canonical discriminator {kind!r}")


def decode_query_responses(
    value: object,
    *,
    graph_dialect: GraphResultDialect = "auto",
    expected_graph_operations: frozenset[str] | None = None,
    expected_graph_queries: Mapping[str, object] | None = None,
) -> QueryResponses:
    """Validate graph results against the request dialect, then decode them.

    Supplying canonical graph query contracts selects the canonical dialect;
    callers cannot accidentally validate those contracts against a legacy
    graph_searches response while leaving ``graph_dialect`` at ``"auto"``.
    """
    if expected_graph_operations is not None and expected_graph_queries is not None:
        raise ValueError("expected_graph_operations and expected_graph_queries are mutually exclusive")
    expected_names = expected_graph_operations
    if expected_graph_queries is not None:
        if graph_dialect not in {"auto", "canonical"}:
            raise ValueError("expected_graph_queries requires the canonical graph dialect")
        graph_dialect = "canonical"
        expected_names = frozenset(expected_graph_queries)

    response = _object(value, "response")
    raw_responses = response.get("responses", _MISSING)
    if raw_responses is _MISSING:
        if expected_names is not None:
            _invalid("response", "is missing responses")
    else:
        responses = _array(raw_responses, "response.responses")
        if expected_names is not None and len(responses) != 1:
            _invalid("response.responses", "must contain exactly one response")
        for response_index, raw_result in enumerate(responses):
            result_path = f"response.responses[{response_index}]"
            result = _object(raw_result, result_path)
            graph_results = result.get("graph_results", _MISSING)
            if graph_results is _MISSING:
                if expected_names:
                    _invalid(result_path, "is missing graph_results")
                continue
            operations = _object(graph_results, f"{result_path}.graph_results")
            if expected_names is not None and frozenset(operations) != expected_names:
                _invalid(
                    f"{result_path}.graph_results",
                    "operation names do not match the request",
                )
            for name, graph_result in operations.items():
                result_kind = graph_result.get("kind", _MISSING) if isinstance(graph_result, Mapping) else _MISSING
                is_canonical = graph_dialect == "canonical" or result_kind in {"bindings", "aggregates", "nodes"}
                if is_canonical and not is_valid_graph_identifier(name):
                    _invalid(f"{result_path}.graph_results", f"contains invalid operation name {name!r}")
                contract = None
                if expected_graph_queries is not None:
                    contract = _canonical_result_contract(
                        expected_graph_queries[name],
                        f"request.graph_queries[{name!r}]",
                    )
                _validate_graph_result(
                    graph_result,
                    f"{result_path}.graph_results[{name!r}]",
                    graph_dialect,
                    contract,
                )
    try:
        return QueryResponses.from_dict(response)
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        raise AntflyException(f"query returned invalid response: {exc}") from exc
