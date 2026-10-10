from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.sql_array_element_type import SQLArrayElementType
from ..models.sql_column_type import SQLColumnType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sql_numeric_modifier import SQLNumericModifier


T = TypeVar("T", bound="SQLColumn")


@_attrs_define
class SQLColumn:
    """
    Attributes:
        name (str): Display label. Labels need not be unique; rows use matching ordinal positions.
        type_ (SQLColumnType): Logical SQL result type. Integer values and numbers with element_type numeric are decimal
            strings to preserve exact precision in every client.
        element_type (SQLArrayElementType | Unset): Bound SQL scalar or array-element identity, including numeric widths
            and exact NUMERIC. Never inferred from JSON value shape.
        numeric_modifier (SQLNumericModifier | Unset): PostgreSQL NUMERIC precision and signed scale. For arrays this
            describes every element, not dimensions. Absent means unconstrained NUMERIC.
    """

    name: str
    type_: SQLColumnType
    element_type: SQLArrayElementType | Unset = UNSET
    numeric_modifier: SQLNumericModifier | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        type_ = self.type_.value

        element_type: str | Unset = UNSET
        if not isinstance(self.element_type, Unset):
            element_type = self.element_type.value

        numeric_modifier: dict[str, Any] | Unset = UNSET
        if not isinstance(self.numeric_modifier, Unset):
            numeric_modifier = self.numeric_modifier.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "type": type_,
            }
        )
        if element_type is not UNSET:
            field_dict["element_type"] = element_type
        if numeric_modifier is not UNSET:
            field_dict["numeric_modifier"] = numeric_modifier

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_numeric_modifier import SQLNumericModifier

        d = dict(src_dict)
        name = d.pop("name")

        type_ = SQLColumnType(d.pop("type"))

        _element_type = d.pop("element_type", UNSET)
        element_type: SQLArrayElementType | Unset
        if isinstance(_element_type, Unset):
            element_type = UNSET
        else:
            element_type = SQLArrayElementType(_element_type)

        _numeric_modifier = d.pop("numeric_modifier", UNSET)
        numeric_modifier: SQLNumericModifier | Unset
        if isinstance(_numeric_modifier, Unset):
            numeric_modifier = UNSET
        else:
            numeric_modifier = SQLNumericModifier.from_dict(_numeric_modifier)

        sql_column = cls(
            name=name,
            type_=type_,
            element_type=element_type,
            numeric_modifier=numeric_modifier,
        )

        return sql_column
