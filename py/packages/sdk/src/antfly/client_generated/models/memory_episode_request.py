from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryEpisodeRequest")


@_attrs_define
class MemoryEpisodeRequest:
    """
    Attributes:
        cursor (None | str | Unset):
        limit (int | Unset):  Default: 32.
        known_at (int | None | Unset):
    """

    cursor: None | str | Unset = UNSET
    limit: int | Unset = 32
    known_at: int | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        cursor: None | str | Unset
        if isinstance(self.cursor, Unset):
            cursor = UNSET
        else:
            cursor = self.cursor

        limit = self.limit

        known_at: int | None | Unset
        if isinstance(self.known_at, Unset):
            known_at = UNSET
        else:
            known_at = self.known_at

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if cursor is not UNSET:
            field_dict["cursor"] = cursor
        if limit is not UNSET:
            field_dict["limit"] = limit
        if known_at is not UNSET:
            field_dict["known_at"] = known_at

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

        limit = d.pop("limit", UNSET)

        def _parse_known_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        known_at = _parse_known_at(d.pop("known_at", UNSET))

        memory_episode_request = cls(
            cursor=cursor,
            limit=limit,
            known_at=known_at,
        )

        return memory_episode_request
