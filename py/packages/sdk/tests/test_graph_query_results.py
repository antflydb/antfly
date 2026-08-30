from typing import Any, cast

import pytest

from antfly.client import AntflyClient
from antfly.client_generated.models.graph_aggregates_result import GraphAggregatesResult
from antfly.client_generated.models.graph_bindings_result import GraphBindingsResult
from antfly.client_generated.models.graph_paths_result import GraphPathsResult
from antfly.client_generated.models.graph_query_unsupported_error import (
    GraphQueryUnsupportedError,
)
from antfly.client_generated.models.graph_query_unsupported_error_reason import (
    GraphQueryUnsupportedErrorReason,
)
from antfly.client_generated.models.legacy_graph_search_result import LegacyGraphSearchResult
from antfly.client_generated.models.stateful_graph_query_results import StatefulGraphQueryResults
from antfly.client_generated.types import Unset
from antfly.exceptions import AntflyException
from antfly.graph_results import decode_query_responses


def _query_response(graph_result: object, operation: str = "result") -> dict[str, object]:
    return {
        "responses": [
            {
                "took": 1,
                "status": 200,
                "graph_results": {operation: graph_result},
            }
        ]
    }


def test_stateful_transport_model_decodes_pre_discriminator_legacy_result() -> None:
    results = StatefulGraphQueryResults.from_dict(
        {
            "neighbors": {
                "type": "neighbors",
                "total": 12,
            }
        }
    )

    result = results["neighbors"]
    assert isinstance(result, LegacyGraphSearchResult)
    assert isinstance(result.kind, Unset)
    assert result.total == 12


def test_canonical_query_decoder_rejects_legacy_or_missing_discriminator() -> None:
    legacy_response = _query_response(
        {"type": "neighbors", "total": 1},
        operation="walk",
    )
    with pytest.raises(AntflyException, match="canonical graph results require a discriminator"):
        decode_query_responses(
            legacy_response,
            graph_dialect="canonical",
            expected_graph_operations=frozenset({"walk"}),
        )

    with pytest.raises(AntflyException, match="must contain exactly one response"):
        decode_query_responses(
            {"responses": []},
            graph_dialect="canonical",
            expected_graph_operations=frozenset({"walk"}),
        )

    with pytest.raises(AntflyException, match="operation names do not match the request"):
        decode_query_responses(
            _query_response(
                {
                    "kind": "nodes",
                    "nodes": [],
                    "stats": {"returned_items": 0, "truncated": False},
                },
                operation="unexpected",
            ),
            graph_dialect="canonical",
            expected_graph_operations=frozenset({"walk"}),
        )


def test_canonical_path_does_not_compute_unused_overflowing_product() -> None:
    path_query = {
        "path": {
            "index": "graph",
            "shortest_path": {"from": {"key": "a"}, "to": {"key": "c"}},
        }
    }
    decode_query_responses(
        _query_response(
            {
                "kind": "paths",
                "paths": [
                    {
                        "path": {
                            "nodes": [{"key": "a"}, {"key": "b"}, {"key": "c"}],
                            "edges": [
                                {
                                    "from": {"key": "a"},
                                    "to": {"key": "b"},
                                    "direction": "out",
                                    "type": "related",
                                    "weight": 1e200,
                                },
                                {
                                    "from": {"key": "b"},
                                    "to": {"key": "c"},
                                    "direction": "out",
                                    "type": "related",
                                    "weight": 1e200,
                                },
                            ],
                            "length": 2,
                            "objective": "min_hops",
                            "weight_sum": 2e200,
                            "objective_value": 2,
                        },
                    }
                ],
                "stats": {"returned_items": 1},
            },
            operation="path",
        ),
        expected_graph_queries=path_query,
    )


def test_canonical_query_decoder_binds_result_shape_to_request() -> None:
    path_query = {
        "path": {
            "index": "graph",
            "shortest_path": {"from": {"key": "a"}, "to": {"key": "b"}},
        }
    }
    with pytest.raises(AntflyException, match="canonical graph results require a discriminator"):
        decode_query_responses(
            _query_response(
                {"type": "shortest_path", "total": 1},
                operation="path",
            ),
            expected_graph_queries=path_query,
        )

    with pytest.raises(ValueError, match="must be auto, canonical, or none"):
        decode_query_responses(
            _query_response(
                {"type": "shortest_path", "total": 1},
                operation="path",
            ),
            graph_dialect=cast(Any, "legacy"),
            expected_graph_queries=path_query,
        )

    with pytest.raises(AntflyException, match="must be 'paths' for the requested operation"):
        decode_query_responses(
            _query_response(
                {
                    "kind": "aggregates",
                    "aggregates": {"count": {"value": "1", "exact": True}},
                    "stats": {"returned_items": 1},
                },
                operation="path",
            ),
            expected_graph_queries=path_query,
        )

    bindings_query = {
        "matched": {
            "index": "graph",
            "match": {
                "anchor": "a",
                "nodes": {"a": {}, "b": {}},
                "edges": [{"from": "a", "to": "b"}],
            },
            "return": {"bindings": ["a", "b"]},
        }
    }
    with pytest.raises(AntflyException, match="binding aliases do not match the requested projection"):
        decode_query_responses(
            _query_response(
                {
                    "kind": "bindings",
                    "rows": [{"a": {"key": "a"}, "c": None}],
                    "stats": {"returned_items": 1, "truncated": False},
                },
                operation="matched",
            ),
            graph_dialect="canonical",
            expected_graph_queries=bindings_query,
        )

    aggregates_query = {
        "counted": {
            "index": "graph",
            "match": {"anchor": "a", "nodes": {"a": {}}, "edges": []},
            "return": {"aggregates": {"rows": {"count": "*"}}},
        }
    }
    with pytest.raises(AntflyException, match="names do not match the requested aggregates"):
        decode_query_responses(
            _query_response(
                {
                    "kind": "aggregates",
                    "aggregates": {"other": {"value": "1", "exact": True}},
                    "stats": {"returned_items": 1},
                },
                operation="counted",
            ),
            graph_dialect="canonical",
            expected_graph_queries=aggregates_query,
        )


def test_canonical_query_decoder_enforces_cardinality_and_path_ownership() -> None:
    zero_hop_path = {
        "nodes": [{"key": "a"}],
        "edges": [],
        "length": 0,
        "objective": "min_hops",
        "weight_sum": 0,
        "objective_value": 0,
    }
    path_query = {
        "path": {
            "index": "graph",
            "shortest_path": {"from": {"key": "a"}, "to": {"key": "a"}},
        }
    }
    malformed_path_results = [
        (
            {
                "kind": "paths",
                "paths": [{"path": zero_hop_path}, {"path": zero_hop_path}],
                "stats": {"returned_items": 2},
            },
            "exceeds the requested result limit",
        ),
        (
            {
                "kind": "paths",
                "paths": [],
                "stats": {"returned_items": 0, "truncated": True},
            },
            "contains unknown member",
        ),
        (
            {
                "kind": "paths",
                "paths": [{"path": zero_hop_path, "unexpected": True}],
                "stats": {"returned_items": 1},
            },
            "contains unknown member",
        ),
    ]
    for result, message in malformed_path_results:
        with pytest.raises(AntflyException, match=message):
            decode_query_responses(
                _query_response(result, operation="path"),
                expected_graph_queries=path_query,
            )

    traversal_query = {
        "walk": {
            "index": "graph",
            "traverse": {"start": {"keys": ["a"]}, "limit": 1},
        }
    }
    with pytest.raises(AntflyException, match="contains a path that was not requested"):
        decode_query_responses(
            _query_response(
                {
                    "kind": "nodes",
                    "nodes": [{"key": "a", "depth": 0, "path": [{"key": "a"}]}],
                    "stats": {"returned_items": 1, "truncated": False},
                },
                operation="walk",
            ),
            expected_graph_queries=traversal_query,
        )

    traversal_query["walk"]["traverse"]["include_paths"] = True
    with pytest.raises(AntflyException, match="is missing its requested path"):
        decode_query_responses(
            _query_response(
                {
                    "kind": "nodes",
                    "nodes": [{"key": "a", "depth": 0}],
                    "stats": {"returned_items": 1, "truncated": False},
                },
                operation="walk",
            ),
            expected_graph_queries=traversal_query,
        )


def test_public_query_decoder_accepts_valid_canonical_results() -> None:
    canonical = decode_query_responses(
        _query_response(
            {
                "kind": "bindings",
                "rows": [{"person": {"key": "person:1"}, "company": None}],
                "stats": {"returned_items": 1, "truncated": False},
            }
        )
    )
    assert not isinstance(canonical.responses, Unset)
    assert not isinstance(canonical.responses[0].graph_results, Unset)
    assert isinstance(canonical.responses[0].graph_results["result"], GraphBindingsResult)

    aggregates = decode_query_responses(
        _query_response(
            {
                "kind": "aggregates",
                "aggregates": {"count": {"value": "340282366920938463463374607431768211455", "exact": True}},
                "stats": {"returned_items": 1},
            }
        )
    )
    assert not isinstance(aggregates.responses, Unset)
    assert not isinstance(aggregates.responses[0].graph_results, Unset)
    assert isinstance(aggregates.responses[0].graph_results["result"], GraphAggregatesResult)

    nodes = decode_query_responses(
        _query_response(
            {
                "kind": "paths",
                "paths": [
                    {
                        "path": {
                            "nodes": [{"key": "a"}, {"key": "b"}],
                            "edges": [
                                {
                                    "from": {"key": "a"},
                                    "to": {"key": "b"},
                                    "direction": "out",
                                    "type": "edge",
                                    "weight": 0.5,
                                }
                            ],
                            "length": 1,
                            "objective": "min_weight_sum",
                            "weight_sum": 0.5,
                            "objective_value": 0.5,
                        },
                    }
                ],
                "stats": {"returned_items": 1},
            }
        )
    )
    assert not isinstance(nodes.responses, Unset)
    assert not isinstance(nodes.responses[0].graph_results, Unset)
    assert isinstance(nodes.responses[0].graph_results["result"], GraphPathsResult)


def test_canonical_query_decoder_rejects_unrequested_documents_but_allows_sparse_hydration() -> None:
    path_query = {
        "path": {
            "index": "graph",
            "shortest_path": {"from": {"key": "a"}, "to": {"key": "b"}},
        }
    }

    def node_result(*, include_document: bool) -> dict[str, object]:
        item: dict[str, object] = {}
        if include_document:
            item["document"] = {"private": True}
        return {
            "kind": "paths",
            "paths": [
                {
                    **item,
                    "path": {
                        "nodes": [{"key": "a"}, {"key": "b"}],
                        "edges": [
                            {
                                "from": {"key": "a"},
                                "to": {"key": "b"},
                                "direction": "out",
                                "type": "related",
                                "weight": 1,
                            }
                        ],
                        "length": 1,
                        "objective": "min_hops",
                        "weight_sum": 1,
                        "objective_value": 1,
                    },
                }
            ],
            "stats": {"returned_items": 1},
        }

    with pytest.raises(AntflyException, match="was returned without being requested"):
        decode_query_responses(
            _query_response(node_result(include_document=True), operation="path"),
            expected_graph_queries=path_query,
        )

    path_query["path"]["shortest_path"]["include_documents"] = True
    decode_query_responses(
        _query_response(node_result(include_document=True), operation="path"),
        expected_graph_queries=path_query,
    )
    decode_query_responses(
        _query_response(node_result(include_document=False), operation="path"),
        expected_graph_queries=path_query,
    )

    bindings_query = {
        "matched": {
            "index": "graph",
            "match": {"anchor": "a", "nodes": {"a": {}}, "edges": []},
            "return": {"bindings": ["a"]},
        }
    }
    binding_result = {
        "kind": "bindings",
        "rows": [{"a": {"key": "a", "document": {"private": True}}}],
        "stats": {"returned_items": 1, "truncated": False},
    }
    with pytest.raises(AntflyException, match="was returned without being requested"):
        decode_query_responses(
            _query_response(binding_result, operation="matched"),
            expected_graph_queries=bindings_query,
        )
    bindings_query["matched"]["return"]["include_documents"] = True
    decode_query_responses(
        _query_response(binding_result, operation="matched"),
        expected_graph_queries=bindings_query,
    )


@pytest.mark.parametrize(
    "graph_result",
    [
        {
            "kind": "bindings",
            "rows": [{}],
            "stats": {"returned_items": 1},
        },
        {
            "kind": "bindings",
            "rows": [{"person": {}}],
            "stats": {"returned_items": 1},
        },
        {
            "kind": "bindings",
            "rows": [{"person": {"key": ""}}],
            "stats": {"returned_items": 1},
        },
        {
            "kind": "bindings",
            "rows": [{"*": {"key": "person:1"}}],
            "stats": {"returned_items": 1},
        },
        {
            "kind": "bindings",
            "rows": [{"person": {"key": "person:1", "unexpected": True}}],
            "stats": {"returned_items": 1, "truncated": False},
        },
        {
            "kind": "bindings",
            "rows": [{"person": {"key": "person:1"}}],
            "stats": {"returned_items": 1, "truncated": False},
            "unexpected": True,
        },
        {
            "kind": "bindings",
            "rows": [{"person": {"key": "person:1"}}],
            "stats": {"returned_items": 0, "truncated": False},
        },
        {
            "kind": "aggregates",
            "aggregates": {"count": {"value": "1", "exact": False}},
            "stats": {"returned_items": 1},
        },
        {
            "kind": "aggregates",
            "aggregates": {"count": {"value": "1.0", "exact": True}},
            "stats": {"returned_items": 1},
        },
        {
            "kind": "aggregates",
            "aggregates": {"count": {"value": "1", "exact": True}},
            "stats": {"returned_items": 1, "truncated": True},
        },
        {
            "kind": "nodes",
            "nodes": [{"key": "b", "depth": 1}],
            "paths": [
                {
                    "nodes": [{"key": "a"}, {"key": "b"}],
                    "edges": [
                        {
                            "from": {"key": "a"},
                            "to": {"key": "wrong"},
                            "direction": "out",
                            "type": "edge",
                            "weight": 0.5,
                        }
                    ],
                    "length": 1,
                    "objective": "min_hops",
                    "weight_sum": 0.5,
                    "objective_value": 1,
                }
            ],
            "stats": {"returned_items": 1, "truncated": False},
        },
        {
            "kind": "nodes",
            "nodes": [{"key": "b", "depth": 1}],
            "paths": [
                {
                    "nodes": [{"key": "a"}, {"key": "b"}],
                    "edges": [
                        {
                            "from": {"key": "a"},
                            "to": {"key": "b"},
                            "direction": "out",
                            "type": "edge",
                            "weight": 0.5,
                        }
                    ],
                    "length": 1,
                    "objective": "min_weight_sum",
                    "weight_sum": 0.25,
                    "objective_value": 0.5,
                }
            ],
            "stats": {"returned_items": 1, "truncated": False},
        },
        {
            "kind": "nodes",
            "nodes": [{"key": "b", "depth": 0, "path": [{"key": "a"}, {"key": "b"}]}],
            "paths": [],
            "stats": {"returned_items": 1, "truncated": False},
        },
        {
            "kind": "nodes",
            "nodes": [{"key": "wrong", "depth": 1, "path": [{"key": "a"}, {"key": "b"}]}],
            "paths": [],
            "stats": {"returned_items": 1, "truncated": False},
        },
        {
            "kind": "nodes",
            "nodes": [],
            "paths": [
                {
                    "nodes": [{"key": "a"}, {"key": "b"}],
                    "edges": [
                        {
                            "from": {"key": "a"},
                            "to": {"key": "b"},
                            "direction": "out",
                            "type": "é" * 32_769,
                            "weight": 1,
                        }
                    ],
                    "length": 1,
                    "objective": "min_hops",
                    "weight_sum": 1,
                    "objective_value": 1,
                }
            ],
            "stats": {"returned_items": 1, "truncated": False},
        },
        {
            "kind": "unknown",
            "stats": {"returned_items": 0, "truncated": False},
        },
    ],
)
def test_public_query_decoder_rejects_malformed_canonical_graph_results(graph_result: object) -> None:
    with pytest.raises(AntflyException, match="invalid graph response"):
        decode_query_responses(_query_response(graph_result))


def test_public_query_decoder_rejects_invalid_canonical_operation_name() -> None:
    with pytest.raises(AntflyException, match="invalid operation name"):
        decode_query_responses(
            _query_response(
                {
                    "kind": "bindings",
                    "rows": [{"person": {"key": "person:1"}}],
                    "stats": {"returned_items": 1, "truncated": False},
                },
                operation="*",
            )
        )


def test_query_decoder_enforces_observable_path_and_traversal_semantics() -> None:
    constrained_query = {
        "path": {
            "index": "graph",
            "shortest_path": {
                "from": {"key": "a", "table": "docs"},
                "to": {"key": "b", "table": "entities"},
                "direction": "in",
                "edge_types": ["cites"],
                "edge_weight": {"max": 0.5},
                "max_depth": 1,
                "objective": "min_weight_sum",
            },
        }
    }
    valid_path = {
        "nodes": [{"key": "a"}, {"key": "b", "table": "entities"}],
        "edges": [
            {
                "from": {"key": "a"},
                "to": {"key": "b", "table": "entities"},
                "direction": "in",
                "type": "cites",
                "weight": 0.5,
            }
        ],
        "length": 1,
        "objective": "min_weight_sum",
        "weight_sum": 0.5,
        "objective_value": 0.5,
    }

    def path_response(graph_path: object) -> dict[str, object]:
        return _query_response(
            {
                "kind": "paths",
                "paths": [{"path": graph_path}],
                "stats": {"returned_items": 1},
            },
            operation="path",
        )

    decode_query_responses(
        path_response(valid_path),
        expected_graph_queries=constrained_query,
        query_table="docs",
    )

    wrong_terminal = {
        **valid_path,
        "nodes": [{"key": "a"}, {"key": "wrong"}],
        "edges": [{**valid_path["edges"][0], "to": {"key": "wrong"}}],
    }
    with pytest.raises(AntflyException, match="terminal endpoint"):
        decode_query_responses(
            path_response(wrong_terminal),
            expected_graph_queries=constrained_query,
            query_table="docs",
        )

    wrong_type = {
        **valid_path,
        "edges": [{**valid_path["edges"][0], "type": "links"}],
    }
    with pytest.raises(AntflyException, match="was not requested"):
        decode_query_responses(
            path_response(wrong_type),
            expected_graph_queries=constrained_query,
            query_table="docs",
        )
    unfiltered_query = {
        "path": {
            **constrained_query["path"],
            "shortest_path": {
                **constrained_query["path"]["shortest_path"],
                "edge_types": [],
            },
        }
    }
    decode_query_responses(
        path_response(wrong_type),
        expected_graph_queries=unfiltered_query,
        query_table="docs",
    )

    traversal_query = {
        "walk": {
            "index": "graph",
            "traverse": {"start": {"keys": ["a"]}, "max_depth": 1},
        }
    }
    with pytest.raises(AntflyException, match="requested traversal identity"):
        decode_query_responses(
            _query_response(
                {
                    "kind": "nodes",
                    "nodes": [{"key": "wrong", "depth": 0}],
                    "stats": {"returned_items": 1, "truncated": False},
                },
                operation="walk",
            ),
            expected_graph_queries=traversal_query,
        )

    k_query = {
        "path": {
            "index": "graph",
            "k_shortest_paths": {"from": {"key": "a"}, "to": {"key": "b"}, "k": 2},
        }
    }
    direct = {
        "nodes": [{"key": "a"}, {"key": "b"}],
        "edges": [
            {
                "from": {"key": "a"},
                "to": {"key": "b"},
                "direction": "out",
                "type": "links",
                "weight": 1,
            }
        ],
        "length": 1,
        "objective": "min_hops",
        "weight_sum": 1,
        "objective_value": 1,
    }
    duplicate_result = {
        "kind": "paths",
        "paths": [{"path": direct}, {"path": direct}],
        "stats": {"returned_items": 2},
    }
    with pytest.raises(AntflyException, match="duplicates an earlier path"):
        decode_query_responses(
            _query_response(duplicate_result, operation="path"),
            expected_graph_queries=k_query,
        )


def test_antfly_client_query_uses_fail_closed_graph_result_decoder(monkeypatch: pytest.MonkeyPatch) -> None:
    client = AntflyClient("http://test")
    malformed = _query_response(
        {
            "kind": "bindings",
            "rows": [{"person": {}}],
            "stats": {"returned_items": 1, "truncated": False},
        }
    )
    monkeypatch.setattr(client, "_request", lambda *_args, **_kwargs: malformed)

    with pytest.raises(AntflyException, match="invalid graph response"):
        client.query("docs")


def test_serverless_legacy_graph_rejection_decodes_as_typed_error() -> None:
    error = GraphQueryUnsupportedError.from_dict(
        {
            "status": 422,
            "error": "graph_query_unsupported",
            "message": "serverless graph queries require graph_queries",
            "retryable": False,
            "operation": "$request",
            "feature": "graph_searches",
            "reason": "legacy_graph_searches_not_supported",
        }
    )

    assert error.reason is GraphQueryUnsupportedErrorReason.LEGACY_GRAPH_SEARCHES_NOT_SUPPORTED


def test_serverless_request_control_rejection_decodes_as_typed_error() -> None:
    error = GraphQueryUnsupportedError.from_dict(
        {
            "status": 422,
            "error": "graph_query_unsupported",
            "message": "this request control cannot be combined with exact graph execution",
            "retryable": False,
            "operation": "$request",
            "feature": "order_by",
            "reason": "request_control_not_supported",
        }
    )

    assert error.operation == "$request"
    assert error.feature == "order_by"
    assert error.reason is GraphQueryUnsupportedErrorReason.REQUEST_CONTROL_NOT_SUPPORTED
