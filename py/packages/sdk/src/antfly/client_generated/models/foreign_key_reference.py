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
        period (str | Unset): Parent application-time period name for temporal `REFERENCES (..., PERIOD period)`
            constraints.
    """

    table: str | Unset = UNSET
    columns: list[str] | Unset = UNSET
    period: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        columns: list[str] | Unset = UNSET
        if not isinstance(self.columns, Unset):
            columns = self.columns

        period = self.period

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if table is not UNSET:
            field_dict["table"] = table
        if columns is not UNSET:
            field_dict["columns"] = columns
        if period is not UNSET:
            field_dict["period"] = period

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table = d.pop("table", UNSET)

        columns = cast(list[str], d.pop("columns", UNSET))

        period = d.pop("period", UNSET)

        foreign_key_reference = cls(
            table=table,
            columns=columns,
            period=period,
        )

        return foreign_key_reference
