from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.rows_expression_field import RowsExpressionField
    from ..models.rows_expression_operator import RowsExpressionOperator
    from ..models.rows_expression_value import RowsExpressionValue


T = TypeVar("T", bound="RowsExpressionAssignmentMap")


@_attrs_define
class RowsExpressionAssignmentMap:
    """Field-to-expression assignment map over the shared row-expression AST."""

    additional_properties: dict[str, RowsExpressionField | RowsExpressionOperator | RowsExpressionValue] = _attrs_field(
        init=False, factory=dict
    )

    def to_dict(self) -> dict[str, Any]:
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_value import RowsExpressionValue

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            if isinstance(prop, RowsExpressionField):
                field_dict[prop_name] = prop.to_dict()
            elif isinstance(prop, RowsExpressionValue):
                field_dict[prop_name] = prop.to_dict()
            else:
                field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_operator import RowsExpressionOperator
        from ..models.rows_expression_value import RowsExpressionValue

        d = dict(src_dict)
        rows_expression_assignment_map = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():

            def _parse_additional_property(
                data: object,
            ) -> RowsExpressionField | RowsExpressionOperator | RowsExpressionValue:
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_rows_expression_type_0 = RowsExpressionField.from_dict(data)

                    return componentsschemas_rows_expression_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_rows_expression_type_1 = RowsExpressionValue.from_dict(data)

                    return componentsschemas_rows_expression_type_1
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_rows_expression_type_2 = RowsExpressionOperator.from_dict(data)

                return componentsschemas_rows_expression_type_2

            additional_property = _parse_additional_property(prop_dict)

            additional_properties[prop_name] = additional_property

        rows_expression_assignment_map.additional_properties = additional_properties
        return rows_expression_assignment_map

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> RowsExpressionField | RowsExpressionOperator | RowsExpressionValue:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: RowsExpressionField | RowsExpressionOperator | RowsExpressionValue) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
