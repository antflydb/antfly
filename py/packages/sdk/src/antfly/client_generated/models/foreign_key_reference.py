from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ForeignKeyReference")


@_attrs_define
class ForeignKeyReference:
    """Parent side of a relational foreign-key constraint.

    Attributes:
        table (str | Unset): Referenced relational table name.
        columns (list[str] | Unset): Referenced parent columns. Use ["_id"] for the document-key primary key, or an
            ordered column tuple backed by a declared unique constraint.
    """

    table: str | Unset = UNSET
    columns: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        columns: list[str] | Unset = UNSET
        if not isinstance(self.columns, Unset):
            columns = self.columns

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if table is not UNSET:
            field_dict["table"] = table
        if columns is not UNSET:
            field_dict["columns"] = columns

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table = d.pop("table", UNSET)

        columns = cast(list[str], d.pop("columns", UNSET))

        foreign_key_reference = cls(
            table=table,
            columns=columns,
        )

        return foreign_key_reference
