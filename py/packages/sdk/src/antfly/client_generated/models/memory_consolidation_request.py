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
    """

    limit: int | Unset = 32
    stale_after_ms: int | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        limit = self.limit

        stale_after_ms: int | None | Unset
        if isinstance(self.stale_after_ms, Unset):
            stale_after_ms = UNSET
        else:
            stale_after_ms = self.stale_after_ms

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if limit is not UNSET:
            field_dict["limit"] = limit
        if stale_after_ms is not UNSET:
            field_dict["stale_after_ms"] = stale_after_ms

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

        memory_consolidation_request = cls(
            limit=limit,
            stale_after_ms=stale_after_ms,
        )

        return memory_consolidation_request
