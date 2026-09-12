from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.graph_metric_edge_filter_mode import GraphMetricEdgeFilterMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphMetricEdgeFilter")


@_attrs_define
class GraphMetricEdgeFilter:
    """Omitting this object selects all edge types. A types list selects only those types; mode and types cannot both be
    supplied.

        Attributes:
            mode (GraphMetricEdgeFilterMode | Unset):
            types (list[str] | Unset):
    """

    mode: GraphMetricEdgeFilterMode | Unset = UNSET
    types: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        types: list[str] | Unset = UNSET
        if not isinstance(self.types, Unset):
            types = self.types

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if mode is not UNSET:
            field_dict["mode"] = mode
        if types is not UNSET:
            field_dict["types"] = types

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _mode = d.pop("mode", UNSET)
        mode: GraphMetricEdgeFilterMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = GraphMetricEdgeFilterMode(_mode)

        types = cast(list[str], d.pop("types", UNSET))

        graph_metric_edge_filter = cls(
            mode=mode,
            types=types,
        )

        return graph_metric_edge_filter
