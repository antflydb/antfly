from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_metric_rerank_score_details import GraphMetricRerankScoreDetails


T = TypeVar("T", bound="QueryScoreDetails")


@_attrs_define
class QueryScoreDetails:
    """Optional score provenance for ranking features that changed the final hit score.

    Attributes:
        graph_metric_rerank (GraphMetricRerankScoreDetails | Unset):
    """

    graph_metric_rerank: GraphMetricRerankScoreDetails | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        graph_metric_rerank: dict[str, Any] | Unset = UNSET
        if not isinstance(self.graph_metric_rerank, Unset):
            graph_metric_rerank = self.graph_metric_rerank.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if graph_metric_rerank is not UNSET:
            field_dict["graph_metric_rerank"] = graph_metric_rerank

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_metric_rerank_score_details import GraphMetricRerankScoreDetails

        d = dict(src_dict)
        _graph_metric_rerank = d.pop("graph_metric_rerank", UNSET)
        graph_metric_rerank: GraphMetricRerankScoreDetails | Unset
        if isinstance(_graph_metric_rerank, Unset):
            graph_metric_rerank = UNSET
        else:
            graph_metric_rerank = GraphMetricRerankScoreDetails.from_dict(_graph_metric_rerank)

        query_score_details = cls(
            graph_metric_rerank=graph_metric_rerank,
        )

        query_score_details.additional_properties = d
        return query_score_details

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
