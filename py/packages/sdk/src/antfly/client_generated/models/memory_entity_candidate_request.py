from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_entity_candidate_request_kind import MemoryEntityCandidateRequestKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryEntityCandidateRequest")


@_attrs_define
class MemoryEntityCandidateRequest:
    """
    Attributes:
        kind (MemoryEntityCandidateRequestKind | Unset):  Default: MemoryEntityCandidateRequestKind.IDENTITY.
        cursor (None | str | Unset):
        limit (int | Unset):  Default: 16.
    """

    kind: MemoryEntityCandidateRequestKind | Unset = MemoryEntityCandidateRequestKind.IDENTITY
    cursor: None | str | Unset = UNSET
    limit: int | Unset = 16

    def to_dict(self) -> dict[str, Any]:
        kind: str | Unset = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

        cursor: None | str | Unset
        if isinstance(self.cursor, Unset):
            cursor = UNSET
        else:
            cursor = self.cursor

        limit = self.limit

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if kind is not UNSET:
            field_dict["kind"] = kind
        if cursor is not UNSET:
            field_dict["cursor"] = cursor
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _kind = d.pop("kind", UNSET)
        kind: MemoryEntityCandidateRequestKind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = MemoryEntityCandidateRequestKind(_kind)

        def _parse_cursor(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        cursor = _parse_cursor(d.pop("cursor", UNSET))

        limit = d.pop("limit", UNSET)

        memory_entity_candidate_request = cls(
            kind=kind,
            cursor=cursor,
            limit=limit,
        )

        return memory_entity_candidate_request
