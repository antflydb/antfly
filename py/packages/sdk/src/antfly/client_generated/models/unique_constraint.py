from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="UniqueConstraint")


@_attrs_define
class UniqueConstraint:
    """Relational unique constraint.

    Attributes:
        name (str | Unset): Constraint name, unique within the table schema.
        columns (list[str] | Unset): Unique columns. One or more ordered non-json relational columns are supported.
    """

    name: str | Unset = UNSET
    columns: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        columns: list[str] | Unset = UNSET
        if not isinstance(self.columns, Unset):
            columns = self.columns

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if name is not UNSET:
            field_dict["name"] = name
        if columns is not UNSET:
            field_dict["columns"] = columns

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name", UNSET)

        columns = cast(list[str], d.pop("columns", UNSET))

        unique_constraint = cls(
            name=name,
            columns=columns,
        )

        return unique_constraint
