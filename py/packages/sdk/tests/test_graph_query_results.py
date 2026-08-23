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
