from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.sql_array_element_type import SQLArrayElementType
from ..models.sql_column_type import SQLColumnType
from ..types import UNSET, Unset

T = TypeVar("T", bound="SQLColumn")


@_attrs_define
class SQLColumn:
    """
    Attributes:
        name (str): Display label. Labels need not be unique; rows use matching ordinal positions.
        type_ (SQLColumnType): Logical SQL result type. Integer values are decimal strings to preserve exact precision
            in every client.
        element_type (SQLArrayElementType | Unset): Bound SQL array element type, including numeric widths. Never
            inferred from JSON value shape.
    """

    name: str
    type_: SQLColumnType
    element_type: SQLArrayElementType | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        type_ = self.type_.value

        element_type: str | Unset = UNSET
        if not isinstance(self.element_type, Unset):
            element_type = self.element_type.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "type": type_,
            }
        )
        if element_type is not UNSET:
            field_dict["element_type"] = element_type

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        type_ = SQLColumnType(d.pop("type"))

        _element_type = d.pop("element_type", UNSET)
        element_type: SQLArrayElementType | Unset
        if isinstance(_element_type, Unset):
            element_type = UNSET
        else:
            element_type = SQLArrayElementType(_element_type)

        sql_column = cls(
            name=name,
            type_=type_,
            element_type=element_type,
        )

        return sql_column
