from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_change_request_start import MemoryChangeRequestStart
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryChangeRequest")


@_attrs_define
class MemoryChangeRequest:
    """
    Attributes:
        cursor (None | str | Unset):
        start (MemoryChangeRequestStart | Unset):  Default: MemoryChangeRequestStart.LATEST.
        limit (int | Unset):  Default: 64.
    """

    cursor: None | str | Unset = UNSET
    start: MemoryChangeRequestStart | Unset = MemoryChangeRequestStart.LATEST
    limit: int | Unset = 64

    def to_dict(self) -> dict[str, Any]:
        cursor: None | str | Unset
        if isinstance(self.cursor, Unset):
            cursor = UNSET
        else:
            cursor = self.cursor

        start: str | Unset = UNSET
        if not isinstance(self.start, Unset):
            start = self.start.value

        limit = self.limit

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if cursor is not UNSET:
            field_dict["cursor"] = cursor
        if start is not UNSET:
            field_dict["start"] = start
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_cursor(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        cursor = _parse_cursor(d.pop("cursor", UNSET))

        _start = d.pop("start", UNSET)
        start: MemoryChangeRequestStart | Unset
        if isinstance(_start, Unset):
            start = UNSET
        else:
            start = MemoryChangeRequestStart(_start)

        limit = d.pop("limit", UNSET)

        memory_change_request = cls(
            cursor=cursor,
            start=start,
            limit=limit,
        )

        return memory_change_request
