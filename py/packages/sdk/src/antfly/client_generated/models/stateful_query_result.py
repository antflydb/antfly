from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query_hits import QueryHits
    from ..models.query_profile import QueryProfile
    from ..models.query_result_base_aggregations import QueryResultBaseAggregations
    from ..models.query_result_base_analyses import QueryResultBaseAnalyses
    from ..models.stateful_graph_query_results import StatefulGraphQueryResults


T = TypeVar("T", bound="StatefulQueryResult")


@_attrs_define
class StatefulQueryResult:
    """Result emitted by the stateful compatibility transport.

    Attributes:
        took (int): Duration of the query in milliseconds.
        status (int): HTTP status code of the query operation.
        hits (QueryHits | Unset): A list of query hits.
        aggregations (QueryResultBaseAggregations | Unset): Aggregation results keyed by the user-defined aggregation
            names from the request.
            Contains computed metrics or buckets depending on the aggregation type.
        analyses (QueryResultBaseAnalyses | Unset): Analysis results like PCA and t-SNE per index embeddings.
        profile (QueryProfile | Unset): Detailed execution profiling for a query. Present in the response
            when the request sets `profile: true`.
        error (str | Unset): Error message if the query failed.
        table (str | Unset): Which table this result came from
        graph_results (StatefulGraphQueryResults | Unset): Stateful graph results keyed by operation name. Legacy values
            are possible only when the corresponding request used graph_searches.
    """

    took: int
    status: int
    hits: QueryHits | Unset = UNSET
    aggregations: QueryResultBaseAggregations | Unset = UNSET
    analyses: QueryResultBaseAnalyses | Unset = UNSET
    profile: QueryProfile | Unset = UNSET
    error: str | Unset = UNSET
    table: str | Unset = UNSET
    graph_results: StatefulGraphQueryResults | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        took = self.took

        status = self.status

        hits: dict[str, Any] | Unset = UNSET
        if not isinstance(self.hits, Unset):
            hits = self.hits.to_dict()

        aggregations: dict[str, Any] | Unset = UNSET
        if not isinstance(self.aggregations, Unset):
            aggregations = self.aggregations.to_dict()

        analyses: dict[str, Any] | Unset = UNSET
        if not isinstance(self.analyses, Unset):
            analyses = self.analyses.to_dict()

        profile: dict[str, Any] | Unset = UNSET
        if not isinstance(self.profile, Unset):
            profile = self.profile.to_dict()

        error = self.error

        table = self.table

        graph_results: dict[str, Any] | Unset = UNSET
        if not isinstance(self.graph_results, Unset):
            graph_results = self.graph_results.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "took": took,
                "status": status,
            }
        )
        if hits is not UNSET:
            field_dict["hits"] = hits
        if aggregations is not UNSET:
            field_dict["aggregations"] = aggregations
        if analyses is not UNSET:
            field_dict["analyses"] = analyses
        if profile is not UNSET:
            field_dict["profile"] = profile
        if error is not UNSET:
            field_dict["error"] = error
        if table is not UNSET:
            field_dict["table"] = table
        if graph_results is not UNSET:
            field_dict["graph_results"] = graph_results

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_hits import QueryHits
        from ..models.query_profile import QueryProfile
        from ..models.query_result_base_aggregations import QueryResultBaseAggregations
        from ..models.query_result_base_analyses import QueryResultBaseAnalyses
        from ..models.stateful_graph_query_results import StatefulGraphQueryResults

        d = dict(src_dict)
        took = d.pop("took")

        status = d.pop("status")

        _hits = d.pop("hits", UNSET)
        hits: QueryHits | Unset
        if isinstance(_hits, Unset):
            hits = UNSET
        else:
            hits = QueryHits.from_dict(_hits)

        _aggregations = d.pop("aggregations", UNSET)
        aggregations: QueryResultBaseAggregations | Unset
        if isinstance(_aggregations, Unset):
            aggregations = UNSET
        else:
            aggregations = QueryResultBaseAggregations.from_dict(_aggregations)

        _analyses = d.pop("analyses", UNSET)
        analyses: QueryResultBaseAnalyses | Unset
        if isinstance(_analyses, Unset):
            analyses = UNSET
        else:
            analyses = QueryResultBaseAnalyses.from_dict(_analyses)

        _profile = d.pop("profile", UNSET)
        profile: QueryProfile | Unset
        if isinstance(_profile, Unset):
            profile = UNSET
        else:
            profile = QueryProfile.from_dict(_profile)

        error = d.pop("error", UNSET)

        table = d.pop("table", UNSET)

        _graph_results = d.pop("graph_results", UNSET)
        graph_results: StatefulGraphQueryResults | Unset
        if isinstance(_graph_results, Unset):
            graph_results = UNSET
        else:
            graph_results = StatefulGraphQueryResults.from_dict(_graph_results)

        stateful_query_result = cls(
            took=took,
            status=status,
            hits=hits,
            aggregations=aggregations,
            analyses=analyses,
            profile=profile,
            error=error,
            table=table,
            graph_results=graph_results,
        )

        stateful_query_result.additional_properties = d
        return stateful_query_result

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
