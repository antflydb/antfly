from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.projection_expression import ProjectionExpression


T = TypeVar("T", bound="QueryRequestSelect")


@_attrs_define
class QueryRequestSelect:
    """Pure named output expressions evaluated after final filtering, ranking and pagination; returned in _computed.
    Compatible with fields, which selects stored _source. Uses the SQL scalar engine and lazy CASE branches. Aliases may
    reference other select aliases with ref; cycles and collisions with evaluate.compute are rejected. Does not change
    hit membership or paging.

    """

    additional_properties: dict[str, ProjectionExpression] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.projection_expression import ProjectionExpression

        d = dict(src_dict)
        query_request_select = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = ProjectionExpression.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        query_request_select.additional_properties = additional_properties
        return query_request_select

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> ProjectionExpression:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: ProjectionExpression) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
