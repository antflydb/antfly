from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.algebraic_aggregation_join_kind import AlgebraicAggregationJoinKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="AlgebraicAggregationJoin")


@_attrs_define
class AlgebraicAggregationJoin:
    """
    Attributes:
        name (str): Algebraic join materialization or capability name
        group_side (str): Join side that supplies grouping bucket values
        measure_side (str): Join side that supplies metric values
        kind (AlgebraicAggregationJoinKind | Unset): Temporal join mode for the algebraic materialization
    """

    name: str
    group_side: str
    measure_side: str
    kind: AlgebraicAggregationJoinKind | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        group_side = self.group_side

        measure_side = self.measure_side

        kind: str | Unset = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "group_side": group_side,
                "measure_side": measure_side,
            }
        )
        if kind is not UNSET:
            field_dict["kind"] = kind

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        group_side = d.pop("group_side")

        measure_side = d.pop("measure_side")

        _kind = d.pop("kind", UNSET)
        kind: AlgebraicAggregationJoinKind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = AlgebraicAggregationJoinKind(_kind)

        algebraic_aggregation_join = cls(
            name=name,
            group_side=group_side,
            measure_side=measure_side,
            kind=kind,
        )

        algebraic_aggregation_join.additional_properties = d
        return algebraic_aggregation_join

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
