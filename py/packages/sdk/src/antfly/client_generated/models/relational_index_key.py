from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.relational_index_key_direction import RelationalIndexKeyDirection
from ..models.relational_index_key_nulls import RelationalIndexKeyNulls
from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalIndexKey")


@_attrs_define
class RelationalIndexKey:
    """Ordered component of a relational ordered-tuple index key.

    Attributes:
        column (str): Declared relational column used by this key component.
        collation (str | Unset): Optional collation used by this ordered key component.
        direction (RelationalIndexKeyDirection | Unset): Sort direction for ordered scans. Omitted defaults to asc.
        nulls (RelationalIndexKeyNulls | Unset): Null placement for ordered scans. Omitted uses method default.
    """

    column: str
    collation: str | Unset = UNSET
    direction: RelationalIndexKeyDirection | Unset = UNSET
    nulls: RelationalIndexKeyNulls | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        column = self.column

        collation = self.collation

        direction: str | Unset = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        nulls: str | Unset = UNSET
        if not isinstance(self.nulls, Unset):
            nulls = self.nulls.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "column": column,
            }
        )
        if collation is not UNSET:
            field_dict["collation"] = collation
        if direction is not UNSET:
            field_dict["direction"] = direction
        if nulls is not UNSET:
            field_dict["nulls"] = nulls

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        column = d.pop("column")

        collation = d.pop("collation", UNSET)

        _direction = d.pop("direction", UNSET)
        direction: RelationalIndexKeyDirection | Unset
        if isinstance(_direction, Unset):
            direction = UNSET
        else:
            direction = RelationalIndexKeyDirection(_direction)

        _nulls = d.pop("nulls", UNSET)
        nulls: RelationalIndexKeyNulls | Unset
        if isinstance(_nulls, Unset):
            nulls = UNSET
        else:
            nulls = RelationalIndexKeyNulls(_nulls)

        relational_index_key = cls(
            column=column,
            collation=collation,
            direction=direction,
            nulls=nulls,
        )

        return relational_index_key
