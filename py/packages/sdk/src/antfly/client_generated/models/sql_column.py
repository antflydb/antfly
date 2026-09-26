from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.sql_column_type import SQLColumnType

T = TypeVar("T", bound="SQLColumn")


@_attrs_define
class SQLColumn:
    """
    Attributes:
        name (str): Display label. Labels need not be unique; rows use matching ordinal positions.
        type_ (SQLColumnType): Logical SQL result type. Integer values are decimal strings to preserve exact precision
            in every client.
    """

    name: str
    type_: SQLColumnType

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        type_ = self.type_.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "type": type_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        type_ = SQLColumnType(d.pop("type"))

        sql_column = cls(
            name=name,
            type_=type_,
        )

        return sql_column
