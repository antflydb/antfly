import pytest

from antfly.client import AntflyClient
from antfly.client_generated.models.graph_aggregates_result import GraphAggregatesResult
from antfly.client_generated.models.graph_bindings_result import GraphBindingsResult
from antfly.client_generated.models.graph_nodes_result import GraphNodesResult
from antfly.client_generated.models.graph_query_unsupported_error import (
    GraphQueryUnsupportedError,
)
from antfly.client_generated.models.graph_query_unsupported_error_reason import (
    GraphQueryUnsupportedErrorReason,
)
from antfly.client_generated.models.legacy_graph_query_result import LegacyGraphQueryResult
from antfly.client_generated.models.query_result_graph_results import QueryResultGraphResults
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


def test_pre_discriminator_graph_result_decodes_as_legacy() -> None:
    results = QueryResultGraphResults.from_dict(
        {
            "neighbors": {
                "type": "neighbors",
                "total": 12,
            }
        }
    )

    result = results["neighbors"]
    assert isinstance(result, LegacyGraphQueryResult)
    assert isinstance(result.kind, Unset)
    assert result.total == 12


def test_public_query_decoder_accepts_valid_canonical_and_legacy_results() -> None:
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
                "stats": {"returned_items": 1, "truncated": False},
            }
        )
    )
    assert not isinstance(aggregates.responses, Unset)
    assert not isinstance(aggregates.responses[0].graph_results, Unset)
    assert isinstance(aggregates.responses[0].graph_results["result"], GraphAggregatesResult)

    nodes = decode_query_responses(
        _query_response(
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
                                "type": "edge",
                                "weight": 0.5,
                            }
                        ],
                        "length": 1,
                        "weight_mode": "min_weight",
                        "weight_sum": 0.5,
                        "objective_value": 0.5,
                    }
                ],
                "stats": {"returned_items": 1, "truncated": False},
            }
        )
    )
    assert not isinstance(nodes.responses, Unset)
    assert not isinstance(nodes.responses[0].graph_results, Unset)
    assert isinstance(nodes.responses[0].graph_results["result"], GraphNodesResult)

    legacy = decode_query_responses(
        _query_response(
            {
                "type": "neighbors",
                "total": 1,
            },
            operation="legacy operation name",
        )
    )
    assert not isinstance(legacy.responses, Unset)
    assert not isinstance(legacy.responses[0].graph_results, Unset)
    assert isinstance(legacy.responses[0].graph_results["legacy operation name"], LegacyGraphQueryResult)


@pytest.mark.parametrize(
    "graph_result",
    [
        {
            "kind": "bindings",
            "rows": [{}],
            "stats": {"returned_items": 1, "truncated": False},
        },
        {
            "kind": "bindings",
            "rows": [{"person": {}}],
            "stats": {"returned_items": 1, "truncated": False},
        },
        {
            "kind": "bindings",
            "rows": [{"person": {"key": ""}}],
            "stats": {"returned_items": 1, "truncated": False},
        },
        {
            "kind": "bindings",
            "rows": [{"*": {"key": "person:1"}}],
            "stats": {"returned_items": 1, "truncated": False},
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
            "stats": {"returned_items": 1, "truncated": False},
        },
        {
            "kind": "aggregates",
            "aggregates": {"count": {"value": "1.0", "exact": True}},
            "stats": {"returned_items": 1, "truncated": False},
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
                            "type": "edge",
                            "weight": 0.5,
                        }
                    ],
                    "length": 1,
                    "weight_mode": "min_hops",
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
                            "type": "edge",
                            "weight": 0.5,
                        }
                    ],
                    "length": 1,
                    "weight_mode": "min_weight",
                    "weight_sum": 0.25,
                    "objective_value": 0.5,
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
