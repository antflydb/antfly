from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryEntityViewRequest")


@_attrs_define
class MemoryEntityViewRequest:
    """
    Attributes:
        limit (int | Unset):  Default: 16.
        known_at (int | None | Unset):
        valid_at (int | None | Unset):
    """

    limit: int | Unset = 16
    known_at: int | None | Unset = UNSET
    valid_at: int | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        limit = self.limit

        known_at: int | None | Unset
        if isinstance(self.known_at, Unset):
            known_at = UNSET
        else:
            known_at = self.known_at

        valid_at: int | None | Unset
        if isinstance(self.valid_at, Unset):
            valid_at = UNSET
        else:
            valid_at = self.valid_at

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if limit is not UNSET:
            field_dict["limit"] = limit
        if known_at is not UNSET:
            field_dict["known_at"] = known_at
        if valid_at is not UNSET:
            field_dict["valid_at"] = valid_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        limit = d.pop("limit", UNSET)

        def _parse_known_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        known_at = _parse_known_at(d.pop("known_at", UNSET))

        def _parse_valid_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        valid_at = _parse_valid_at(d.pop("valid_at", UNSET))

        memory_entity_view_request = cls(
            limit=limit,
            known_at=known_at,
            valid_at=valid_at,
        )

        return memory_entity_view_request
