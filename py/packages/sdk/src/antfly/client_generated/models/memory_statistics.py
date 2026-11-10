from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_space_stats import MemorySpaceStats


T = TypeVar("T", bound="MemoryStatistics")


@_attrs_define
class MemoryStatistics:
    """
    Attributes:
        stats (MemorySpaceStats | Unset):
        next_cursor (None | str | Unset):
        complete (bool | Unset):  Default: True.
        scanned (int | Unset):  Default: 0.
    """

    stats: MemorySpaceStats | Unset = UNSET
    next_cursor: None | str | Unset = UNSET
    complete: bool | Unset = True
    scanned: int | Unset = 0

    def to_dict(self) -> dict[str, Any]:
        stats: dict[str, Any] | Unset = UNSET
        if not isinstance(self.stats, Unset):
            stats = self.stats.to_dict()

        next_cursor: None | str | Unset
        if isinstance(self.next_cursor, Unset):
            next_cursor = UNSET
        else:
            next_cursor = self.next_cursor

        complete = self.complete

        scanned = self.scanned

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if stats is not UNSET:
            field_dict["stats"] = stats
        if next_cursor is not UNSET:
            field_dict["next_cursor"] = next_cursor
        if complete is not UNSET:
            field_dict["complete"] = complete
        if scanned is not UNSET:
            field_dict["scanned"] = scanned

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_space_stats import MemorySpaceStats

        d = dict(src_dict)
        _stats = d.pop("stats", UNSET)
        stats: MemorySpaceStats | Unset
        if isinstance(_stats, Unset):
            stats = UNSET
        else:
            stats = MemorySpaceStats.from_dict(_stats)

        def _parse_next_cursor(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        next_cursor = _parse_next_cursor(d.pop("next_cursor", UNSET))

        complete = d.pop("complete", UNSET)

        scanned = d.pop("scanned", UNSET)

        memory_statistics = cls(
            stats=stats,
            next_cursor=next_cursor,
            complete=complete,
            scanned=scanned,
        )

        return memory_statistics
