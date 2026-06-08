from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_unique_predicate_group import RowsUniquePredicateGroup


T = TypeVar("T", bound="RowsConflictUniqueTarget")


@_attrs_define
class RowsConflictUniqueTarget:
    """Declared unique constraint target for `ON CONFLICT`.

    Attributes:
        name (str): Unique constraint name.
        where (RowsUniquePredicateGroup | Unset): Conjunction of partial-unique predicate atoms.
    """

    name: str
    where: RowsUniquePredicateGroup | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        where: dict[str, Any] | Unset = UNSET
        if not isinstance(self.where, Unset):
            where = self.where.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
            }
        )
        if where is not UNSET:
            field_dict["where"] = where

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_unique_predicate_group import RowsUniquePredicateGroup

        d = dict(src_dict)
        name = d.pop("name")

        _where = d.pop("where", UNSET)
        where: RowsUniquePredicateGroup | Unset
        if isinstance(_where, Unset):
            where = UNSET
        else:
            where = RowsUniquePredicateGroup.from_dict(_where)

        rows_conflict_unique_target = cls(
            name=name,
            where=where,
        )

        return rows_conflict_unique_target
