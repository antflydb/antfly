from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.rows_expression import RowsExpression


T = TypeVar("T", bound="RowsExpressionAssignmentMap")


@_attrs_define
class RowsExpressionAssignmentMap:
    """Field-to-expression assignment map over the shared row-expression AST."""

    additional_properties: dict[str, RowsExpression] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression import RowsExpression

        d = dict(src_dict)
        rows_expression_assignment_map = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = RowsExpression.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        rows_expression_assignment_map.additional_properties = additional_properties
        return rows_expression_assignment_map

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> RowsExpression:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: RowsExpression) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
