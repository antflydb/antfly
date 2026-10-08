from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.sql_array_element_type import SQLArrayElementType
from ..models.sql_column_type import SQLColumnType
from ..types import UNSET, Unset

T = TypeVar("T", bound="SQLParameterDescriptor")


@_attrs_define
class SQLParameterDescriptor:
    """Immutable positional input contract. Element identity also preserves primitive widths; array inputs require it.
    Unknown slots have no SQL constraint.

        Attributes:
            type_ (SQLColumnType): Logical SQL result type. Integer values and numbers with element_type numeric are decimal
                strings to preserve exact precision in every client.
            nullable (bool):
            element_type (SQLArrayElementType | Unset): Bound SQL scalar or array-element identity, including numeric widths
                and exact NUMERIC. Never inferred from JSON value shape.
    """

    type_: SQLColumnType
    nullable: bool
    element_type: SQLArrayElementType | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        nullable = self.nullable

        element_type: str | Unset = UNSET
        if not isinstance(self.element_type, Unset):
            element_type = self.element_type.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "nullable": nullable,
            }
        )
        if element_type is not UNSET:
            field_dict["element_type"] = element_type

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = SQLColumnType(d.pop("type"))

        nullable = d.pop("nullable")

        _element_type = d.pop("element_type", UNSET)
        element_type: SQLArrayElementType | Unset
        if isinstance(_element_type, Unset):
            element_type = UNSET
        else:
            element_type = SQLArrayElementType(_element_type)

        sql_parameter_descriptor = cls(
            type_=type_,
            nullable=nullable,
            element_type=element_type,
        )

        return sql_parameter_descriptor
