from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphEdgeWeightRange")


@_attrs_define
class GraphEdgeWeightRange:
    """Inclusive per-edge weight filter. At least one bound is required. Bounds must be finite and non-negative; when both
    are present, min must not exceed max. This filters individual stored edges and does not constrain the aggregate path
    objective.

        Attributes:
            min_ (float | Unset):
            max_ (float | Unset):
    """

    min_: float | Unset = UNSET
    max_: float | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        min_ = self.min_

        max_ = self.max_

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if min_ is not UNSET:
            field_dict["min"] = min_
        if max_ is not UNSET:
            field_dict["max"] = max_

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        min_ = d.pop("min", UNSET)

        max_ = d.pop("max", UNSET)

        graph_edge_weight_range = cls(
            min_=min_,
            max_=max_,
        )

        return graph_edge_weight_range
