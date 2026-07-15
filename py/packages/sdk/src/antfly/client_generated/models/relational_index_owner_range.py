from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalIndexOwnerRange")


@_attrs_define
class RelationalIndexOwnerRange:
    """Owner-range coverage for a relational access-method generation.

    Attributes:
        start (str): Inclusive owner range start key.
        end (str): Exclusive owner range end key; empty string means unbounded.
        range_id (str | Unset): Optional stable range identifier.
        placement_generation (int | Unset): Placement generation that produced this range assignment.
    """

    start: str
    end: str
    range_id: str | Unset = UNSET
    placement_generation: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        start = self.start

        end = self.end

        range_id = self.range_id

        placement_generation = self.placement_generation

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "start": start,
                "end": end,
            }
        )
        if range_id is not UNSET:
            field_dict["range_id"] = range_id
        if placement_generation is not UNSET:
            field_dict["placement_generation"] = placement_generation

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        start = d.pop("start")

        end = d.pop("end")

        range_id = d.pop("range_id", UNSET)

        placement_generation = d.pop("placement_generation", UNSET)

        relational_index_owner_range = cls(
            start=start,
            end=end,
            range_id=range_id,
            placement_generation=placement_generation,
        )

        return relational_index_owner_range
