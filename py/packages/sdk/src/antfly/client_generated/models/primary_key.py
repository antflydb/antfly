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
        name (str | Unset): Optional durable primary-key constraint name used by DDL and conflict-target resolution.
        columns (list[str] | Unset): Primary-key columns. One or more ordered required non-json relational columns are
            supported.
        without_overlaps_period (str | Unset): Application-time period name for primary-key `WITHOUT OVERLAPS` temporal
            uniqueness.
    """

    name: str | Unset = UNSET
    columns: list[str] | Unset = UNSET
    without_overlaps_period: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        columns: list[str] | Unset = UNSET
        if not isinstance(self.columns, Unset):
            columns = self.columns

        without_overlaps_period = self.without_overlaps_period

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if name is not UNSET:
            field_dict["name"] = name
        if columns is not UNSET:
            field_dict["columns"] = columns
        if without_overlaps_period is not UNSET:
            field_dict["without_overlaps_period"] = without_overlaps_period

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name", UNSET)

        columns = cast(list[str], d.pop("columns", UNSET))

        without_overlaps_period = d.pop("without_overlaps_period", UNSET)

        primary_key = cls(
            name=name,
            columns=columns,
            without_overlaps_period=without_overlaps_period,
        )

        return primary_key
