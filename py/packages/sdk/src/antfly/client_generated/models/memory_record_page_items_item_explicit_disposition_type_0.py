from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_record_page_items_item_explicit_disposition_type_0_lifecycle import (
    MemoryRecordPageItemsItemExplicitDispositionType0Lifecycle,
)

T = TypeVar("T", bound="MemoryRecordPageItemsItemExplicitDispositionType0")


@_attrs_define
class MemoryRecordPageItemsItemExplicitDispositionType0:
    """
    Attributes:
        lifecycle (MemoryRecordPageItemsItemExplicitDispositionType0Lifecycle):
        valid_to (int | None):
        superseded_by (None | str):
        recorded_to (int | None):
        conflicts_with (list[str]):
        reason (None | str):
    """

    lifecycle: MemoryRecordPageItemsItemExplicitDispositionType0Lifecycle
    valid_to: int | None
    superseded_by: None | str
    recorded_to: int | None
    conflicts_with: list[str]
    reason: None | str

    def to_dict(self) -> dict[str, Any]:
        lifecycle = self.lifecycle.value

        valid_to: int | None
        valid_to = self.valid_to

        superseded_by: None | str
        superseded_by = self.superseded_by

        recorded_to: int | None
        recorded_to = self.recorded_to

        conflicts_with = self.conflicts_with

        reason: None | str
        reason = self.reason

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "lifecycle": lifecycle,
                "valid_to": valid_to,
                "superseded_by": superseded_by,
                "recorded_to": recorded_to,
                "conflicts_with": conflicts_with,
                "reason": reason,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        lifecycle = MemoryRecordPageItemsItemExplicitDispositionType0Lifecycle(d.pop("lifecycle"))

        def _parse_valid_to(data: object) -> int | None:
            if data is None:
                return data
            return cast(int | None, data)

        valid_to = _parse_valid_to(d.pop("valid_to"))

        def _parse_superseded_by(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        superseded_by = _parse_superseded_by(d.pop("superseded_by"))

        def _parse_recorded_to(data: object) -> int | None:
            if data is None:
                return data
            return cast(int | None, data)

        recorded_to = _parse_recorded_to(d.pop("recorded_to"))

        conflicts_with = cast(list[str], d.pop("conflicts_with"))

        def _parse_reason(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        reason = _parse_reason(d.pop("reason"))

        memory_record_page_items_item_explicit_disposition_type_0 = cls(
            lifecycle=lifecycle,
            valid_to=valid_to,
            superseded_by=superseded_by,
            recorded_to=recorded_to,
            conflicts_with=conflicts_with,
            reason=reason,
        )

        return memory_record_page_items_item_explicit_disposition_type_0
