from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.algebraic_aggregate_config import AlgebraicAggregateConfig


T = TypeVar("T", bound="AlgebraicIndexConfig")


@_attrs_define
class AlgebraicIndexConfig:
    """Schema-derived algebraic index capabilities with optional declarative aggregate recipes. Physical materialization
    state remains engine-owned.

        Attributes:
            derive_from_schema (bool | Unset): When true, derive typed fields and capabilities from the table schema.
                Physical fields, laws, joins and state remain engine-owned.
            aggregates (list[AlgebraicAggregateConfig] | Unset): Desired exact aggregate recipes over schema column names.
                Eligible SQL automatically reuses complete, snapshot-bound materializations; unsupported SQL shapes retain
                scanning.
    """

    derive_from_schema: bool | Unset = UNSET
    aggregates: list[AlgebraicAggregateConfig] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        derive_from_schema = self.derive_from_schema

        aggregates: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.aggregates, Unset):
            aggregates = []
            for aggregates_item_data in self.aggregates:
                aggregates_item = aggregates_item_data.to_dict()
                aggregates.append(aggregates_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if derive_from_schema is not UNSET:
            field_dict["derive_from_schema"] = derive_from_schema
        if aggregates is not UNSET:
            field_dict["aggregates"] = aggregates

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.algebraic_aggregate_config import AlgebraicAggregateConfig

        d = dict(src_dict)
        derive_from_schema = d.pop("derive_from_schema", UNSET)

        _aggregates = d.pop("aggregates", UNSET)
        aggregates: list[AlgebraicAggregateConfig] | Unset = UNSET
        if _aggregates is not UNSET:
            aggregates = []
            for aggregates_item_data in _aggregates:
                aggregates_item = AlgebraicAggregateConfig.from_dict(aggregates_item_data)

                aggregates.append(aggregates_item)

        algebraic_index_config = cls(
            derive_from_schema=derive_from_schema,
            aggregates=aggregates,
        )

        algebraic_index_config.additional_properties = d
        return algebraic_index_config

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
