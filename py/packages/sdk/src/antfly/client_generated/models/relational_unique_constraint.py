from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalUniqueConstraint")


@_attrs_define
class RelationalUniqueConstraint:
    """A named, ordered composite unique key. Validation status is maintained
    by the server. TTL expiry uses the distributed integrity coordinator.
    Referenced unique keys are nondeferrable.

        Attributes:
            name (str):
            columns (list[str]):
            nulls_not_distinct (bool | Unset): When true, NULL components compare equal for uniqueness.
    """

    name: str
    columns: list[str]
    nulls_not_distinct: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        columns = self.columns

        nulls_not_distinct = self.nulls_not_distinct

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "columns": columns,
            }
        )
        if nulls_not_distinct is not UNSET:
            field_dict["nulls_not_distinct"] = nulls_not_distinct

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        columns = cast(list[str], d.pop("columns"))

        nulls_not_distinct = d.pop("nulls_not_distinct", UNSET)

        relational_unique_constraint = cls(
            name=name,
            columns=columns,
            nulls_not_distinct=nulls_not_distinct,
        )

        return relational_unique_constraint
