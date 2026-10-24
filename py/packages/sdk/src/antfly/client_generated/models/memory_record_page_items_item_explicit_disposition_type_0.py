from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_record_page_items_item_explicit_disposition_type_0_lifecycle import (
    MemoryRecordPageItemsItemExplicitDispositionType0Lifecycle,
)
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_record_page_items_item_explicit_disposition_type_0_provenance_type_0 import (
        MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0,
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
        provenance (MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0 | None | Unset):
        superseded_valid_at (int | None | Unset):
    """

    lifecycle: MemoryRecordPageItemsItemExplicitDispositionType0Lifecycle
    valid_to: int | None
    superseded_by: None | str
    recorded_to: int | None
    conflicts_with: list[str]
    reason: None | str
    provenance: MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0 | None | Unset = UNSET
    superseded_valid_at: int | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_record_page_items_item_explicit_disposition_type_0_provenance_type_0 import (
            MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0,
        )

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

        provenance: dict[str, Any] | None | Unset
        if isinstance(self.provenance, Unset):
            provenance = UNSET
        elif isinstance(self.provenance, MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0):
            provenance = self.provenance.to_dict()
        else:
            provenance = self.provenance

        superseded_valid_at: int | None | Unset
        if isinstance(self.superseded_valid_at, Unset):
            superseded_valid_at = UNSET
        else:
            superseded_valid_at = self.superseded_valid_at

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
        if provenance is not UNSET:
            field_dict["provenance"] = provenance
        if superseded_valid_at is not UNSET:
            field_dict["superseded_valid_at"] = superseded_valid_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_record_page_items_item_explicit_disposition_type_0_provenance_type_0 import (
            MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0,
        )

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

        def _parse_provenance(
            data: object,
        ) -> MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                provenance_type_0 = MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0.from_dict(data)

                return provenance_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0 | None | Unset, data)

        provenance = _parse_provenance(d.pop("provenance", UNSET))

        def _parse_superseded_valid_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        superseded_valid_at = _parse_superseded_valid_at(d.pop("superseded_valid_at", UNSET))

        memory_record_page_items_item_explicit_disposition_type_0 = cls(
            lifecycle=lifecycle,
            valid_to=valid_to,
            superseded_by=superseded_by,
            recorded_to=recorded_to,
            conflicts_with=conflicts_with,
            reason=reason,
            provenance=provenance,
            superseded_valid_at=superseded_valid_at,
        )

        return memory_record_page_items_item_explicit_disposition_type_0
