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
_MAX_GRAPH_PATHS = 100
_DEFAULT_TRAVERSAL_DEPTH = 1
_DEFAULT_PATH_DEPTH = 10
_MISSING = object()
GraphResultDialect = Literal["auto", "canonical", "none"]
GraphResultKind = Literal["bindings", "aggregates", "nodes", "paths"]
GraphNodeResultMode = Literal["traversal", "shortest_path", "k_shortest_paths"]


class _CanonicalResultContract(NamedTuple):
    kind: GraphResultKind
    names: frozenset[str] | None = None
    max_items: int | None = None
    node_mode: GraphNodeResultMode | None = None
    include_paths: bool = False
    include_documents: bool = False
    max_depth: int | None = None
    query_table: str | None = None
    from_endpoint: tuple[str | None, str] | None = None
    to_endpoint: tuple[str | None, str] | None = None
    objective: str | None = None
    direction: str | None = None
    edge_types: frozenset[str] | None = None
    edge_weight_min: float | None = None
    edge_weight_max: float | None = None
    starts: tuple[tuple[str | None, str], ...] | None = None


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


def _table_qualifier(value: object, path: str) -> str:
    result = _nonempty_string(value, path)
    if not any(char not in " \t\r\n" for char in result):
        _invalid(path, "must contain a non-whitespace character")
    return result


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
        _table_qualifier(endpoint["table"], f"{path}.table")
    return endpoint


def _endpoint_identity(value: Mapping[str, Any]) -> tuple[str | None, str]:
    table = value.get("table")
    return (table if isinstance(table, str) else None, value["key"])


def _contract_endpoint(value: object, path: str, query_table: str | None) -> tuple[str | None, str]:
    endpoint = _validate_endpoint(value, path)
    table, key = _endpoint_identity(endpoint)
    return (None if table == query_table else table, key)


def _endpoint_matches_contract(
    actual: Mapping[str, Any],
    expected: tuple[str | None, str],
    query_table: str | None,
) -> bool:
    actual_table, actual_key = _endpoint_identity(actual)
    expected_table, expected_key = expected
    if actual_key != expected_key:
        return False
    if actual_table == expected_table:
        return True
    # The standalone decoder cannot distinguish an explicitly requested table
    # from the implicit query table. High-level clients always provide it.
    return query_table is None and actual_table is None and expected_table is not None


def _operation_edge_contract(
    operation: Mapping[str, Any],
    path: str,
) -> tuple[str, frozenset[str] | None, float | None, float | None]:
    direction = operation.get("direction", "out")
    if direction not in {"out", "in", "both"}:
        _invalid(f"{path}.direction", "must be out, in, or both")
    raw_types = operation.get("edge_types")
    edge_types = None
    if raw_types is not None:
        edge_types = frozenset(
            _nonempty_string(value, f"{path}.edge_types[{index}]", max_utf8_bytes=65_536)
            for index, value in enumerate(_array(raw_types, f"{path}.edge_types"))
        )
    minimum = maximum = None
    if "edge_weight" in operation:
        weight = _object(operation["edge_weight"], f"{path}.edge_weight")
        if "min" in weight:
            minimum = _finite_nonnegative(weight["min"], f"{path}.edge_weight.min")
        if "max" in weight:
            maximum = _finite_nonnegative(weight["max"], f"{path}.edge_weight.max")
        if minimum is not None and maximum is not None and minimum > maximum:
            _invalid(f"{path}.edge_weight", "minimum must not exceed maximum")
    return direction, edge_types, minimum, maximum


def _validate_edge_contract(
    edge: Mapping[str, Any],
    path: str,
    contract: _CanonicalResultContract,
) -> None:
    if contract.direction != "both" and edge["direction"] != contract.direction:
        _invalid(f"{path}.direction", "does not match the requested direction")
    if contract.edge_types and edge["type"] not in contract.edge_types:
        _invalid(f"{path}.type", "was not requested")
    weight = float(edge["weight"])
    if contract.edge_weight_min is not None and weight < contract.edge_weight_min:
        _invalid(f"{path}.weight", "is below the requested minimum")
    if contract.edge_weight_max is not None and weight > contract.edge_weight_max:
        _invalid(f"{path}.weight", "exceeds the requested maximum")


def _validate_path_edge(
    value: object,
    path: str,
    expected_from: Mapping[str, Any],
    expected_to: Mapping[str, Any],
    *,
    max_weight_product: bool,
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
    weight = _finite_nonnegative(edge["weight"], f"{path}.weight", at_most_one=max_weight_product)
    if "metadata" in edge:
        _object(edge["metadata"], f"{path}.metadata")
    return weight


def _float_equal(left: float, right: float) -> bool:
    if not isfinite(left) or not isfinite(right):
        return False
    return abs(left - right) <= 1e-12 * max(1.0, abs(left), abs(right))


def _validate_path(value: object, path: str) -> Mapping[str, Any]:
    graph_path = _object(value, path)
    _exact_keys(
        graph_path,
        path,
        required=frozenset({"nodes", "edges", "length", "objective", "weight_sum", "objective_value"}),
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
    objective_mode = graph_path["objective"]
    if objective_mode not in {"min_hops", "min_weight_sum", "max_weight_product"}:
        _invalid(f"{path}.objective", "has an unknown value")

    weight_sum = 0.0
    weight_product = 1.0
    for index, edge in enumerate(raw_edges):
        weight = _validate_path_edge(
            edge,
            f"{path}.edges[{index}]",
            nodes[index],
            nodes[index + 1],
            max_weight_product=objective_mode == "max_weight_product",
        )
        weight_sum += weight
        if not isfinite(weight_sum):
            _invalid(path, "path score overflowed")
        if objective_mode == "max_weight_product":
            weight_product *= weight
            if not isfinite(weight_product):
                _invalid(path, "path score overflowed")

    encoded_sum = _finite_nonnegative(graph_path["weight_sum"], f"{path}.weight_sum")
    encoded_objective = _finite_nonnegative(graph_path["objective_value"], f"{path}.objective_value")
    if not _float_equal(encoded_sum, weight_sum):
        _invalid(f"{path}.weight_sum", "does not equal the sum of edge weights")
    objective = (
        float(length)
        if objective_mode == "min_hops"
        else weight_product
        if objective_mode == "max_weight_product"
        else weight_sum
    )
    if not _float_equal(encoded_objective, objective):
        _invalid(f"{path}.objective_value", "does not match objective")
    return graph_path


def _validate_path_contract(
    graph_path: Mapping[str, Any],
    path: str,
    contract: _CanonicalResultContract,
) -> None:
    length = graph_path["length"]
    if contract.max_depth is not None and length > contract.max_depth:
        _invalid(f"{path}.length", "exceeds the requested max_depth")
    if graph_path["objective"] != contract.objective:
        _invalid(f"{path}.objective", "does not match the requested objective")
    nodes = _array(graph_path["nodes"], f"{path}.nodes")
    first = _object(nodes[0], f"{path}.nodes[0]")
    last = _object(nodes[-1], f"{path}.nodes[{len(nodes) - 1}]")
    if contract.from_endpoint is not None and not _endpoint_matches_contract(
        first, contract.from_endpoint, contract.query_table
    ):
        _invalid(f"{path}.nodes[0]", "does not match the requested start endpoint")
    if contract.to_endpoint is not None and not _endpoint_matches_contract(
        last, contract.to_endpoint, contract.query_table
    ):
        _invalid(f"{path}.nodes[{len(nodes) - 1}]", "does not match the requested terminal endpoint")
    for index, raw_edge in enumerate(_array(graph_path["edges"], f"{path}.edges")):
        _validate_edge_contract(_object(raw_edge, f"{path}.edges[{index}]"), f"{path}.edges[{index}]", contract)


def _path_is_loopless(graph_path: Mapping[str, Any]) -> bool:
    identities = [_endpoint_identity(_object(node, "path.nodes")) for node in _array(graph_path["nodes"], "path.nodes")]
    return len(identities) == len(set(identities))


def _path_signature(graph_path: Mapping[str, Any]) -> tuple[object, ...]:
    nodes = tuple(_endpoint_identity(_object(node, "path.nodes")) for node in _array(graph_path["nodes"], "path.nodes"))
    edges = tuple(
        (
            _object(edge, "path.edges")["direction"],
            _object(edge, "path.edges")["type"],
        )
        for edge in _array(graph_path["edges"], "path.edges")
    )
    return nodes, edges


def _validate_path_collection(
    paths: list[Mapping[str, Any]],
    path: str,
    contract: _CanonicalResultContract,
) -> None:
    if contract.node_mode != "k_shortest_paths":
        return
    seen: set[tuple[object, ...]] = set()
    previous: float | None = None
    for index, graph_path in enumerate(paths):
        if not _path_is_loopless(graph_path):
            _invalid(f"{path}[{index}].path", "must be loopless for k_shortest_paths")
        signature = _path_signature(graph_path)
        if signature in seen:
            _invalid(f"{path}[{index}].path", "duplicates an earlier path")
        seen.add(signature)
        score = float(graph_path["objective_value"])
        if previous is not None:
            descending = contract.objective == "max_weight_product"
            if (descending and score > previous and not _float_equal(score, previous)) or (
                not descending and score < previous and not _float_equal(score, previous)
            ):
                _invalid(f"{path}[{index}].path.objective_value", "is out of objective order")
        previous = score


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
        _table_qualifier(node["table"], f"{path}.table")
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
                max_weight_product=False,
            )
    return node


def _validate_result_node_contract(
    node: Mapping[str, Any],
    path: str,
    contract: _CanonicalResultContract,
) -> None:
    depth = node["depth"]
    if contract.max_depth is not None and depth > contract.max_depth:
        _invalid(f"{path}.depth", "exceeds the requested max_depth")
    starts = contract.starts
    if starts is not None:
        root: Mapping[str, Any] | None = None
        if "path" in node:
            raw_path = _array(node["path"], f"{path}.path")
            root = _object(raw_path[0], f"{path}.path[0]")
        elif depth == 0:
            root = node
        if root is not None and not any(
            _endpoint_matches_contract(root, start, contract.query_table) for start in starts
        ):
            _invalid(path, "does not originate at a requested traversal identity")
    for index, raw_edge in enumerate(_array(node.get("path_edges", []), f"{path}.path_edges")):
        _validate_edge_contract(
            _object(raw_edge, f"{path}.path_edges[{index}]"),
            f"{path}.path_edges[{index}]",
            contract,
        )


def _validate_stats(value: object, path: str, expected_items: int, *, bounded: bool) -> None:
    stats = _object(value, path)
    required = frozenset({"returned_items", "truncated"}) if bounded else frozenset({"returned_items"})
    _exact_keys(stats, path, required=required)
    returned_items = _bounded_integer(stats["returned_items"], f"{path}.returned_items", 0, _MAX_GRAPH_ITEMS)
    if returned_items != expected_items:
        _invalid(f"{path}.returned_items", "does not match the result payload")
    if bounded and type(stats["truncated"]) is not bool:
        _invalid(f"{path}.truncated", "must be a boolean")


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
                _table_qualifier(binding["table"], f"{binding_path}.table")
            if "document" in binding:
                _object(binding["document"], f"{binding_path}.document")
    _validate_stats(value["stats"], f"{path}.stats", len(rows), bounded=True)


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
    _validate_stats(value["stats"], f"{path}.stats", len(aggregates), bounded=False)


def _validate_nodes_result(value: Mapping[str, Any], path: str) -> None:
    _exact_keys(value, path, required=frozenset({"kind", "nodes", "stats"}))
    raw_nodes = _array(value["nodes"], f"{path}.nodes")
    if len(raw_nodes) > _MAX_GRAPH_ITEMS:
        _invalid(path, f"nodes must contain at most {_MAX_GRAPH_ITEMS} items")
    [_validate_result_node(node, f"{path}.nodes[{index}]") for index, node in enumerate(raw_nodes)]
    _validate_stats(value["stats"], f"{path}.stats", len(raw_nodes), bounded=True)


def _validate_paths_result(value: Mapping[str, Any], path: str) -> None:
    _exact_keys(value, path, required=frozenset({"kind", "paths", "stats"}))
    raw_paths = _array(value["paths"], f"{path}.paths")
    if len(raw_paths) > _MAX_GRAPH_PATHS:
        _invalid(path, f"paths must contain at most {_MAX_GRAPH_PATHS} items")
    for index, raw_item in enumerate(raw_paths):
        item_path = f"{path}.paths[{index}]"
        item = _object(raw_item, item_path)
        _exact_keys(item, item_path, required=frozenset({"path"}), optional=frozenset({"document"}))
        _validate_path(item["path"], f"{item_path}.path")
        if "document" in item:
            _object(item["document"], f"{item_path}.document")
    _validate_stats(value["stats"], f"{path}.stats", len(raw_paths), bounded=False)


def _canonical_result_contract(
    value: object,
    path: str,
    query_table: str | None = None,
) -> _CanonicalResultContract:
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
        max_depth = _bounded_integer(
            traversal.get("max_depth", _DEFAULT_TRAVERSAL_DEPTH),
            f"{path}.traverse.max_depth",
            0,
            _MAX_GRAPH_EDGES,
        )
        direction, edge_types, weight_min, weight_max = _operation_edge_contract(traversal, f"{path}.traverse")
        selector = _object(traversal.get("start", _MISSING), f"{path}.traverse.start")
        starts: tuple[tuple[str | None, str], ...] | None
        if "keys" in selector:
            starts = tuple(
                (None, _nonempty_string(key, f"{path}.traverse.start.keys[{index}]"))
                for index, key in enumerate(_array(selector["keys"], f"{path}.traverse.start.keys"))
            )
        elif "identities" in selector:
            starts = tuple(
                _contract_endpoint(endpoint, f"{path}.traverse.start.identities[{index}]", query_table)
                for index, endpoint in enumerate(_array(selector["identities"], f"{path}.traverse.start.identities"))
            )
        else:
            # A result_ref is resolved by the server and is not observable in
            # the response contract without materializing the referenced rows.
            starts = None
        return _CanonicalResultContract(
            "nodes",
            max_items=limit,
            node_mode="traversal",
            include_paths=traversal.get("include_paths") is True,
            include_documents=traversal.get("include_documents") is True,
            max_depth=max_depth,
            query_table=query_table,
            direction=direction,
            edge_types=edge_types,
            edge_weight_min=weight_min,
            edge_weight_max=weight_max,
            starts=starts,
        )
    if "shortest_path" in operation:
        shortest_path = _object(operation["shortest_path"], f"{path}.shortest_path")
        direction, edge_types, weight_min, weight_max = _operation_edge_contract(shortest_path, f"{path}.shortest_path")
        objective = shortest_path.get("objective", "min_hops")
        if objective not in {"min_hops", "min_weight_sum", "max_weight_product"}:
            _invalid(f"{path}.shortest_path.objective", "has an unknown value")
        return _CanonicalResultContract(
            "paths",
            max_items=1,
            node_mode="shortest_path",
            include_documents=shortest_path.get("include_documents") is True,
            max_depth=_bounded_integer(
                shortest_path.get("max_depth", _DEFAULT_PATH_DEPTH),
                f"{path}.shortest_path.max_depth",
                1,
                _MAX_GRAPH_EDGES,
            ),
            query_table=query_table,
            from_endpoint=_contract_endpoint(
                shortest_path.get("from", _MISSING), f"{path}.shortest_path.from", query_table
            ),
            to_endpoint=_contract_endpoint(shortest_path.get("to", _MISSING), f"{path}.shortest_path.to", query_table),
            objective=objective,
            direction=direction,
            edge_types=edge_types,
            edge_weight_min=weight_min,
            edge_weight_max=weight_max,
        )
    if "k_shortest_paths" in operation:
        k_shortest_paths = _object(operation["k_shortest_paths"], f"{path}.k_shortest_paths")
        k = _bounded_integer(k_shortest_paths.get("k", _MISSING), f"{path}.k_shortest_paths.k", 1, 100)
        direction, edge_types, weight_min, weight_max = _operation_edge_contract(
            k_shortest_paths, f"{path}.k_shortest_paths"
        )
        objective = k_shortest_paths.get("objective", "min_hops")
        if objective not in {"min_hops", "min_weight_sum", "max_weight_product"}:
            _invalid(f"{path}.k_shortest_paths.objective", "has an unknown value")
        return _CanonicalResultContract(
            "paths",
            max_items=k,
            node_mode="k_shortest_paths",
            include_documents=k_shortest_paths.get("include_documents") is True,
            max_depth=_bounded_integer(
                k_shortest_paths.get("max_depth", _DEFAULT_PATH_DEPTH),
                f"{path}.k_shortest_paths.max_depth",
                1,
                _MAX_GRAPH_EDGES,
            ),
            query_table=query_table,
            from_endpoint=_contract_endpoint(
                k_shortest_paths.get("from", _MISSING), f"{path}.k_shortest_paths.from", query_table
            ),
            to_endpoint=_contract_endpoint(
                k_shortest_paths.get("to", _MISSING), f"{path}.k_shortest_paths.to", query_table
            ),
            objective=objective,
            direction=direction,
            edge_types=edge_types,
            edge_weight_min=weight_min,
            edge_weight_max=weight_max,
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
        _invalid(f"{path}.kind", "canonical graph results require a discriminator")
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
            if contract.max_items is None or contract.node_mode != "traversal":
                _invalid(path, "has no node operation contract")
            if len(raw_nodes) > contract.max_items:
                _invalid(path, "exceeds the requested result limit")
            if not contract.include_documents:
                for index, raw_node in enumerate(raw_nodes):
                    if "document" in _object(raw_node, f"{path}.nodes[{index}]"):
                        _invalid(f"{path}.nodes[{index}].document", "was returned without being requested")
            if contract.node_mode == "traversal":
                for index, raw_node in enumerate(raw_nodes):
                    node = _object(raw_node, f"{path}.nodes[{index}]")
                    if contract.include_paths:
                        if "path" not in node:
                            _invalid(f"{path}.nodes[{index}]", "is missing its requested path")
                    else:
                        if "path" in node or "path_edges" in node:
                            _invalid(f"{path}.nodes[{index}]", "contains a path that was not requested")
                    _validate_result_node_contract(node, f"{path}.nodes[{index}]", contract)
    elif kind == "paths":
        _validate_paths_result(result, path)
        if contract is not None:
            if contract.node_mode not in {"shortest_path", "k_shortest_paths"} or contract.max_items is None:
                _invalid(path, "has no path operation contract")
            raw_paths = _array(result["paths"], f"{path}.paths")
            if len(raw_paths) > contract.max_items:
                _invalid(path, "exceeds the requested result limit")
            validated_paths: list[Mapping[str, Any]] = []
            if not contract.include_documents:
                for index, raw_item in enumerate(raw_paths):
                    item = _object(raw_item, f"{path}.paths[{index}]")
                    if "document" in item:
                        _invalid(f"{path}.paths[{index}].document", "was returned without being requested")
                    graph_path = _object(item["path"], f"{path}.paths[{index}].path")
                    _validate_path_contract(graph_path, f"{path}.paths[{index}].path", contract)
                    validated_paths.append(graph_path)
            else:
                for index, raw_item in enumerate(raw_paths):
                    item = _object(raw_item, f"{path}.paths[{index}]")
                    graph_path = _object(item["path"], f"{path}.paths[{index}].path")
                    _validate_path_contract(graph_path, f"{path}.paths[{index}].path", contract)
                    validated_paths.append(graph_path)
            _validate_path_collection(validated_paths, f"{path}.paths", contract)
    else:
        _invalid(f"{path}.kind", f"has unknown canonical discriminator {kind!r}")


def decode_query_responses(
    value: object,
    *,
    graph_dialect: GraphResultDialect = "auto",
    expected_graph_operations: frozenset[str] | None = None,
    expected_graph_queries: Mapping[str, object] | None = None,
    query_table: str | None = None,
) -> QueryResponses:
    """Validate canonical graph results against their request, then decode them."""
    if graph_dialect not in {"auto", "canonical", "none"}:
        raise ValueError("graph_dialect must be auto, canonical, or none")
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
                if graph_dialect != "none" and not is_valid_graph_identifier(name):
                    _invalid(f"{result_path}.graph_results", f"contains invalid operation name {name!r}")
                contract = None
                if expected_graph_queries is not None:
                    contract = _canonical_result_contract(
                        expected_graph_queries[name],
                        f"request.graph_queries[{name!r}]",
                        query_table,
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
