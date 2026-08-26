from antfly.client_generated.models.graph_query_unsupported_error import (
    GraphQueryUnsupportedError,
)
from antfly.client_generated.models.graph_query_unsupported_error_reason import (
    GraphQueryUnsupportedErrorReason,
)
from antfly.client_generated.models.legacy_graph_query_result import LegacyGraphQueryResult
from antfly.client_generated.models.query_result_graph_results import QueryResultGraphResults
from antfly.client_generated.types import Unset


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
