from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="RowsFieldAliasProjection")


@_attrs_define
class RowsFieldAliasProjection:
    """Compact field alias projection over a declared column.

    Attributes:
        as_ (str): Output field name.
        field (str): Declared column to project.
    """

    as_: str
    field: str

    def to_dict(self) -> dict[str, Any]:
        as_ = self.as_

        field = self.field

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "as": as_,
                "field": field,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        as_ = d.pop("as")

        field = d.pop("field")

        rows_field_alias_projection = cls(
            as_=as_,
            field=field,
        )

        return rows_field_alias_projection
