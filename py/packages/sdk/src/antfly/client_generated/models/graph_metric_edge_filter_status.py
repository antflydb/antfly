from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_metric_edge_filter_status_mode import GraphMetricEdgeFilterStatusMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphMetricEdgeFilterStatus")


@_attrs_define
class GraphMetricEdgeFilterStatus:
    """
    Attributes:
        mode (GraphMetricEdgeFilterStatusMode):
        types (list[str] | Unset):
    """

    mode: GraphMetricEdgeFilterStatusMode
    types: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        mode = self.mode.value

        types: list[str] | Unset = UNSET
        if not isinstance(self.types, Unset):
            types = self.types

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "mode": mode,
            }
        )
        if types is not UNSET:
            field_dict["types"] = types

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        mode = GraphMetricEdgeFilterStatusMode(d.pop("mode"))

        types = cast(list[str], d.pop("types", UNSET))

        graph_metric_edge_filter_status = cls(
            mode=mode,
            types=types,
        )

        graph_metric_edge_filter_status.additional_properties = d
        return graph_metric_edge_filter_status

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
