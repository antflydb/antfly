from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_record_page_items_item_lifecycle import MemoryRecordPageItemsItemLifecycle
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_record_page_items_item_assertion import MemoryRecordPageItemsItemAssertion


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
        recorded_at (int | Unset):
        lifecycle (MemoryRecordPageItemsItemLifecycle | Unset):
        stale (bool | Unset):
        reason (None | str | Unset):
        conflicts_with (list[str] | Unset):
        recorded_to (int | None | Unset):
        superseded_by (None | str | Unset):
        embedding_generation (int | Unset):
        next_embedding_generation (int | None | Unset):
        next_embedding (list[float] | None | Unset):
    """

    assertion: MemoryRecordPageItemsItemAssertion
    owner: str
    recorded_from: int
    revision: int
    config_generation: int
    input_hash: str | Unset = UNSET
    asserted_valid_to: int | None | Unset = UNSET
    recorded_at: int | Unset = UNSET
    lifecycle: MemoryRecordPageItemsItemLifecycle | Unset = UNSET
    stale: bool | Unset = UNSET
    reason: None | str | Unset = UNSET
    conflicts_with: list[str] | Unset = UNSET
    recorded_to: int | None | Unset = UNSET
    superseded_by: None | str | Unset = UNSET
    embedding_generation: int | Unset = UNSET
    next_embedding_generation: int | None | Unset = UNSET
    next_embedding: list[float] | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
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

        embedding_generation = self.embedding_generation

        next_embedding_generation: int | None | Unset
        if isinstance(self.next_embedding_generation, Unset):
            next_embedding_generation = UNSET
        else:
            next_embedding_generation = self.next_embedding_generation

        next_embedding: list[float] | None | Unset
        if isinstance(self.next_embedding, Unset):
            next_embedding = UNSET
        elif isinstance(self.next_embedding, list):
            next_embedding = self.next_embedding

        else:
            next_embedding = self.next_embedding

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
        if embedding_generation is not UNSET:
            field_dict["embedding_generation"] = embedding_generation
        if next_embedding_generation is not UNSET:
            field_dict["next_embedding_generation"] = next_embedding_generation
        if next_embedding is not UNSET:
            field_dict["next_embedding"] = next_embedding

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_record_page_items_item_assertion import MemoryRecordPageItemsItemAssertion

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

        embedding_generation = d.pop("embedding_generation", UNSET)

        def _parse_next_embedding_generation(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        next_embedding_generation = _parse_next_embedding_generation(d.pop("next_embedding_generation", UNSET))

        def _parse_next_embedding(data: object) -> list[float] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                next_embedding_type_0 = cast(list[float], data)

                return next_embedding_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[float] | None | Unset, data)

        next_embedding = _parse_next_embedding(d.pop("next_embedding", UNSET))

        memory_record_page_items_item = cls(
            assertion=assertion,
            owner=owner,
            recorded_from=recorded_from,
            revision=revision,
            config_generation=config_generation,
            input_hash=input_hash,
            asserted_valid_to=asserted_valid_to,
            recorded_at=recorded_at,
            lifecycle=lifecycle,
            stale=stale,
            reason=reason,
            conflicts_with=conflicts_with,
            recorded_to=recorded_to,
            superseded_by=superseded_by,
            embedding_generation=embedding_generation,
            next_embedding_generation=next_embedding_generation,
            next_embedding=next_embedding,
        )

        return memory_record_page_items_item
