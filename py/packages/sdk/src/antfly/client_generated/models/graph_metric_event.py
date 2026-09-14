from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_metric_event_kind import GraphMetricEventKind

T = TypeVar("T", bound="GraphMetricEvent")


@_attrs_define
class GraphMetricEvent:
    """
    Attributes:
        sequence (int):
        kind (GraphMetricEventKind):
        at_ms (int):
        target_edge_generation (int):
        published_generation (int):
        score_count (int):
    """

    sequence: int
    kind: GraphMetricEventKind
    at_ms: int
    target_edge_generation: int
    published_generation: int
    score_count: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        sequence = self.sequence

        kind = self.kind.value

        at_ms = self.at_ms

        target_edge_generation = self.target_edge_generation

        published_generation = self.published_generation

        score_count = self.score_count

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "sequence": sequence,
                "kind": kind,
                "at_ms": at_ms,
                "target_edge_generation": target_edge_generation,
                "published_generation": published_generation,
                "score_count": score_count,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        sequence = d.pop("sequence")

        kind = GraphMetricEventKind(d.pop("kind"))

        at_ms = d.pop("at_ms")

        target_edge_generation = d.pop("target_edge_generation")

        published_generation = d.pop("published_generation")

        score_count = d.pop("score_count")

        graph_metric_event = cls(
            sequence=sequence,
            kind=kind,
            at_ms=at_ms,
            target_edge_generation=target_edge_generation,
            published_generation=published_generation,
            score_count=score_count,
        )

        graph_metric_event.additional_properties = d
        return graph_metric_event

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
