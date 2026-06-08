from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_conflict_unique_target import RowsConflictUniqueTarget


T = TypeVar("T", bound="RowsConflictTarget")


@_attrs_define
class RowsConflictTarget:
    """Primary-key or named unique constraint conflict target.

    Attributes:
        primary (bool | Unset): Set to `true` to target the declared primary key.
        unique (RowsConflictUniqueTarget | Unset): Declared unique constraint target for `ON CONFLICT`.
    """

    primary: bool | Unset = UNSET
    unique: RowsConflictUniqueTarget | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        primary = self.primary

        unique: dict[str, Any] | Unset = UNSET
        if not isinstance(self.unique, Unset):
            unique = self.unique.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if primary is not UNSET:
            field_dict["primary"] = primary
        if unique is not UNSET:
            field_dict["unique"] = unique

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_conflict_unique_target import RowsConflictUniqueTarget

        d = dict(src_dict)
        primary = d.pop("primary", UNSET)

        _unique = d.pop("unique", UNSET)
        unique: RowsConflictUniqueTarget | Unset
        if isinstance(_unique, Unset):
            unique = UNSET
        else:
            unique = RowsConflictUniqueTarget.from_dict(_unique)

        rows_conflict_target = cls(
            primary=primary,
            unique=unique,
        )

        return rows_conflict_target
