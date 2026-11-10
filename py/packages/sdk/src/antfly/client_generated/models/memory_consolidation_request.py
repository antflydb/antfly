from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryConsolidationRequest")


@_attrs_define
class MemoryConsolidationRequest:
    """
    Attributes:
        limit (int | Unset):  Default: 32.
        stale_after_ms (int | None | Unset):
        cursor (None | str | Unset):
    """

    limit: int | Unset = 32
    stale_after_ms: int | None | Unset = UNSET
    cursor: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        limit = self.limit

        stale_after_ms: int | None | Unset
        if isinstance(self.stale_after_ms, Unset):
            stale_after_ms = UNSET
        else:
            stale_after_ms = self.stale_after_ms

        cursor: None | str | Unset
        if isinstance(self.cursor, Unset):
            cursor = UNSET
        else:
            cursor = self.cursor

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if limit is not UNSET:
            field_dict["limit"] = limit
        if stale_after_ms is not UNSET:
            field_dict["stale_after_ms"] = stale_after_ms
        if cursor is not UNSET:
            field_dict["cursor"] = cursor

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        limit = d.pop("limit", UNSET)

        def _parse_stale_after_ms(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        stale_after_ms = _parse_stale_after_ms(d.pop("stale_after_ms", UNSET))

        def _parse_cursor(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        cursor = _parse_cursor(d.pop("cursor", UNSET))

        memory_consolidation_request = cls(
            limit=limit,
            stale_after_ms=stale_after_ms,
            cursor=cursor,
        )

        return memory_consolidation_request
