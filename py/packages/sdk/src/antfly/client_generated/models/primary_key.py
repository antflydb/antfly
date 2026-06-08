from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="PrimaryKey")


@_attrs_define
class PrimaryKey:
    """Relational primary-key constraint.

    Attributes:
        columns (list[str] | Unset): Primary-key columns. One or more ordered required non-json relational columns are
            supported.
    """

    columns: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        columns: list[str] | Unset = UNSET
        if not isinstance(self.columns, Unset):
            columns = self.columns

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if columns is not UNSET:
            field_dict["columns"] = columns

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        columns = cast(list[str], d.pop("columns", UNSET))

        primary_key = cls(
            columns=columns,
        )

        return primary_key
