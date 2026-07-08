from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.query_hits_total_relation import QueryHitsTotalRelation

T = TypeVar("T", bound="QueryHitsTotal")


@_attrs_define
class QueryHitsTotal:
    """Total hit count metadata.

    Attributes:
        value (int): Hit count value.
        relation (QueryHitsTotalRelation): Whether value is exact or a lower bound.
    """

    value: int
    relation: QueryHitsTotalRelation
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        value = self.value

        relation = self.relation.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "value": value,
                "relation": relation,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        value = d.pop("value")

        relation = QueryHitsTotalRelation(d.pop("relation"))

        query_hits_total = cls(
            value=value,
            relation=relation,
        )

        query_hits_total.additional_properties = d
        return query_hits_total

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
