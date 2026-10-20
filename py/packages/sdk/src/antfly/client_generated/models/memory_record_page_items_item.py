from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_record_page_items_item_lifecycle import MemoryRecordPageItemsItemLifecycle
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_record_page_items_item_assertion import MemoryRecordPageItemsItemAssertion
    from ..models.memory_record_page_items_item_explicit_disposition_type_0 import (
        MemoryRecordPageItemsItemExplicitDispositionType0,
    )


T = TypeVar("T", bound="MemoryRecordPageItemsItem")


@_attrs_define
class MemoryRecordPageItemsItem:
    """
    Attributes:
        assertion (MemoryRecordPageItemsItemAssertion):
        owner (str):
        recorded_from (int):
        revision (int):
        config_generation (int):
        input_hash (str | Unset):
        asserted_valid_to (int | None | Unset):
        explicit_disposition (MemoryRecordPageItemsItemExplicitDispositionType0 | None | Unset):
        recorded_at (int | Unset):
        lifecycle (MemoryRecordPageItemsItemLifecycle | Unset):
        stale (bool | Unset):
        reason (None | str | Unset):
        conflicts_with (list[str] | Unset):
        recorded_to (int | None | Unset):
        superseded_by (None | str | Unset):
        superseded_valid_at (int | None | Unset):
    """

    assertion: MemoryRecordPageItemsItemAssertion
    owner: str
    recorded_from: int
    revision: int
    config_generation: int
    input_hash: str | Unset = UNSET
    asserted_valid_to: int | None | Unset = UNSET
    explicit_disposition: MemoryRecordPageItemsItemExplicitDispositionType0 | None | Unset = UNSET
    recorded_at: int | Unset = UNSET
    lifecycle: MemoryRecordPageItemsItemLifecycle | Unset = UNSET
    stale: bool | Unset = UNSET
    reason: None | str | Unset = UNSET
    conflicts_with: list[str] | Unset = UNSET
    recorded_to: int | None | Unset = UNSET
    superseded_by: None | str | Unset = UNSET
    superseded_valid_at: int | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_record_page_items_item_explicit_disposition_type_0 import (
            MemoryRecordPageItemsItemExplicitDispositionType0,
        )

        assertion = self.assertion.to_dict()

        owner = self.owner

        recorded_from = self.recorded_from

        revision = self.revision

        config_generation = self.config_generation

        input_hash = self.input_hash

        asserted_valid_to: int | None | Unset
        if isinstance(self.asserted_valid_to, Unset):
            asserted_valid_to = UNSET
        else:
            asserted_valid_to = self.asserted_valid_to

        explicit_disposition: dict[str, Any] | None | Unset
        if isinstance(self.explicit_disposition, Unset):
            explicit_disposition = UNSET
        elif isinstance(self.explicit_disposition, MemoryRecordPageItemsItemExplicitDispositionType0):
            explicit_disposition = self.explicit_disposition.to_dict()
        else:
            explicit_disposition = self.explicit_disposition

        recorded_at = self.recorded_at

        lifecycle: str | Unset = UNSET
        if not isinstance(self.lifecycle, Unset):
            lifecycle = self.lifecycle.value

        stale = self.stale

        reason: None | str | Unset
        if isinstance(self.reason, Unset):
            reason = UNSET
        else:
            reason = self.reason

        conflicts_with: list[str] | Unset = UNSET
        if not isinstance(self.conflicts_with, Unset):
            conflicts_with = self.conflicts_with

        recorded_to: int | None | Unset
        if isinstance(self.recorded_to, Unset):
            recorded_to = UNSET
        else:
            recorded_to = self.recorded_to

        superseded_by: None | str | Unset
        if isinstance(self.superseded_by, Unset):
            superseded_by = UNSET
        else:
            superseded_by = self.superseded_by

        superseded_valid_at: int | None | Unset
        if isinstance(self.superseded_valid_at, Unset):
            superseded_valid_at = UNSET
        else:
            superseded_valid_at = self.superseded_valid_at

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "assertion": assertion,
                "owner": owner,
                "recorded_from": recorded_from,
                "revision": revision,
                "config_generation": config_generation,
            }
        )
        if input_hash is not UNSET:
            field_dict["input_hash"] = input_hash
        if asserted_valid_to is not UNSET:
            field_dict["asserted_valid_to"] = asserted_valid_to
        if explicit_disposition is not UNSET:
            field_dict["explicit_disposition"] = explicit_disposition
        if recorded_at is not UNSET:
            field_dict["recorded_at"] = recorded_at
        if lifecycle is not UNSET:
            field_dict["lifecycle"] = lifecycle
        if stale is not UNSET:
            field_dict["stale"] = stale
        if reason is not UNSET:
            field_dict["reason"] = reason
        if conflicts_with is not UNSET:
            field_dict["conflicts_with"] = conflicts_with
        if recorded_to is not UNSET:
            field_dict["recorded_to"] = recorded_to
        if superseded_by is not UNSET:
            field_dict["superseded_by"] = superseded_by
        if superseded_valid_at is not UNSET:
            field_dict["superseded_valid_at"] = superseded_valid_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_record_page_items_item_assertion import MemoryRecordPageItemsItemAssertion
        from ..models.memory_record_page_items_item_explicit_disposition_type_0 import (
            MemoryRecordPageItemsItemExplicitDispositionType0,
        )

        d = dict(src_dict)
        assertion = MemoryRecordPageItemsItemAssertion.from_dict(d.pop("assertion"))

        owner = d.pop("owner")

        recorded_from = d.pop("recorded_from")

        revision = d.pop("revision")

        config_generation = d.pop("config_generation")

        input_hash = d.pop("input_hash", UNSET)

        def _parse_asserted_valid_to(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        asserted_valid_to = _parse_asserted_valid_to(d.pop("asserted_valid_to", UNSET))

        def _parse_explicit_disposition(
            data: object,
        ) -> MemoryRecordPageItemsItemExplicitDispositionType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                explicit_disposition_type_0 = MemoryRecordPageItemsItemExplicitDispositionType0.from_dict(data)

                return explicit_disposition_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryRecordPageItemsItemExplicitDispositionType0 | None | Unset, data)

        explicit_disposition = _parse_explicit_disposition(d.pop("explicit_disposition", UNSET))

        recorded_at = d.pop("recorded_at", UNSET)

        _lifecycle = d.pop("lifecycle", UNSET)
        lifecycle: MemoryRecordPageItemsItemLifecycle | Unset
        if isinstance(_lifecycle, Unset):
            lifecycle = UNSET
        else:
            lifecycle = MemoryRecordPageItemsItemLifecycle(_lifecycle)

        stale = d.pop("stale", UNSET)

        def _parse_reason(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        reason = _parse_reason(d.pop("reason", UNSET))

        conflicts_with = cast(list[str], d.pop("conflicts_with", UNSET))

        def _parse_recorded_to(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        recorded_to = _parse_recorded_to(d.pop("recorded_to", UNSET))

        def _parse_superseded_by(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        superseded_by = _parse_superseded_by(d.pop("superseded_by", UNSET))

        def _parse_superseded_valid_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        superseded_valid_at = _parse_superseded_valid_at(d.pop("superseded_valid_at", UNSET))

        memory_record_page_items_item = cls(
            assertion=assertion,
            owner=owner,
            recorded_from=recorded_from,
            revision=revision,
            config_generation=config_generation,
            input_hash=input_hash,
            asserted_valid_to=asserted_valid_to,
            explicit_disposition=explicit_disposition,
            recorded_at=recorded_at,
            lifecycle=lifecycle,
            stale=stale,
            reason=reason,
            conflicts_with=conflicts_with,
            recorded_to=recorded_to,
            superseded_by=superseded_by,
            superseded_valid_at=superseded_valid_at,
        )

        return memory_record_page_items_item
