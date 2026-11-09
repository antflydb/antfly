from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_count import MemoryCount


T = TypeVar("T", bound="MemorySpaceStats")


@_attrs_define
class MemorySpaceStats:
    """
    Attributes:
        memories (int | Unset):  Default: 0.
        by_kind (list[MemoryCount] | Unset):
        by_lifecycle (list[MemoryCount] | Unset):
        by_stability (list[MemoryCount] | Unset):
        by_producer (list[MemoryCount] | Unset):
        pinned (int | Unset):  Default: 0.
        entities (int | Unset):  Default: 0.
    """

    memories: int | Unset = 0
    by_kind: list[MemoryCount] | Unset = UNSET
    by_lifecycle: list[MemoryCount] | Unset = UNSET
    by_stability: list[MemoryCount] | Unset = UNSET
    by_producer: list[MemoryCount] | Unset = UNSET
    pinned: int | Unset = 0
    entities: int | Unset = 0

    def to_dict(self) -> dict[str, Any]:
        memories = self.memories

        by_kind: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.by_kind, Unset):
            by_kind = []
            for by_kind_item_data in self.by_kind:
                by_kind_item = by_kind_item_data.to_dict()
                by_kind.append(by_kind_item)

        by_lifecycle: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.by_lifecycle, Unset):
            by_lifecycle = []
            for by_lifecycle_item_data in self.by_lifecycle:
                by_lifecycle_item = by_lifecycle_item_data.to_dict()
                by_lifecycle.append(by_lifecycle_item)

        by_stability: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.by_stability, Unset):
            by_stability = []
            for by_stability_item_data in self.by_stability:
                by_stability_item = by_stability_item_data.to_dict()
                by_stability.append(by_stability_item)

        by_producer: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.by_producer, Unset):
            by_producer = []
            for by_producer_item_data in self.by_producer:
                by_producer_item = by_producer_item_data.to_dict()
                by_producer.append(by_producer_item)

        pinned = self.pinned

        entities = self.entities

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if memories is not UNSET:
            field_dict["memories"] = memories
        if by_kind is not UNSET:
            field_dict["by_kind"] = by_kind
        if by_lifecycle is not UNSET:
            field_dict["by_lifecycle"] = by_lifecycle
        if by_stability is not UNSET:
            field_dict["by_stability"] = by_stability
        if by_producer is not UNSET:
            field_dict["by_producer"] = by_producer
        if pinned is not UNSET:
            field_dict["pinned"] = pinned
        if entities is not UNSET:
            field_dict["entities"] = entities

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_count import MemoryCount

        d = dict(src_dict)
        memories = d.pop("memories", UNSET)

        _by_kind = d.pop("by_kind", UNSET)
        by_kind: list[MemoryCount] | Unset = UNSET
        if _by_kind is not UNSET:
            by_kind = []
            for by_kind_item_data in _by_kind:
                by_kind_item = MemoryCount.from_dict(by_kind_item_data)

                by_kind.append(by_kind_item)

        _by_lifecycle = d.pop("by_lifecycle", UNSET)
        by_lifecycle: list[MemoryCount] | Unset = UNSET
        if _by_lifecycle is not UNSET:
            by_lifecycle = []
            for by_lifecycle_item_data in _by_lifecycle:
                by_lifecycle_item = MemoryCount.from_dict(by_lifecycle_item_data)

                by_lifecycle.append(by_lifecycle_item)

        _by_stability = d.pop("by_stability", UNSET)
        by_stability: list[MemoryCount] | Unset = UNSET
        if _by_stability is not UNSET:
            by_stability = []
            for by_stability_item_data in _by_stability:
                by_stability_item = MemoryCount.from_dict(by_stability_item_data)

                by_stability.append(by_stability_item)

        _by_producer = d.pop("by_producer", UNSET)
        by_producer: list[MemoryCount] | Unset = UNSET
        if _by_producer is not UNSET:
            by_producer = []
            for by_producer_item_data in _by_producer:
                by_producer_item = MemoryCount.from_dict(by_producer_item_data)

                by_producer.append(by_producer_item)

        pinned = d.pop("pinned", UNSET)

        entities = d.pop("entities", UNSET)

        memory_space_stats = cls(
            memories=memories,
            by_kind=by_kind,
            by_lifecycle=by_lifecycle,
            by_stability=by_stability,
            by_producer=by_producer,
            pinned=pinned,
            entities=entities,
        )

        return memory_space_stats
