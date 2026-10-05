from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.query_expression import QueryExpression


T = TypeVar("T", bound="QueryEvaluationCompute")


@_attrs_define
class QueryEvaluationCompute:
    """ """

    additional_properties: dict[str, QueryExpression] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_expression import QueryExpression

        d = dict(src_dict)
        query_evaluation_compute = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = QueryExpression.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        query_evaluation_compute.additional_properties = additional_properties
        return query_evaluation_compute

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> QueryExpression:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: QueryExpression) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
