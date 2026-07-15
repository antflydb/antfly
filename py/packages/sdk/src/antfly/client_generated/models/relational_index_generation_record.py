from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.relational_index_generation_record_lifecycle import RelationalIndexGenerationRecordLifecycle
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_index_owner_range import RelationalIndexOwnerRange


T = TypeVar("T", bound="RelationalIndexGenerationRecord")


@_attrs_define
class RelationalIndexGenerationRecord:
    """Shared lifecycle record for derived relational access-method generations.

    Attributes:
        generation (int): Monotonic physical access-method generation.
        owner_ranges (list[RelationalIndexOwnerRange]): Owner ranges covered by this generation.
        lifecycle (RelationalIndexGenerationRecordLifecycle): Lifecycle state for this generation.
        lag (int): Remaining catch-up lag for this generation.
        ready_watermark (int): Durable watermark that is ready for serving.
        failure_reason (str | Unset): Typed or operator-facing failure reason when the lifecycle is not healthy.
    """

    generation: int
    owner_ranges: list[RelationalIndexOwnerRange]
    lifecycle: RelationalIndexGenerationRecordLifecycle
    lag: int
    ready_watermark: int
    failure_reason: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        generation = self.generation

        owner_ranges = []
        for owner_ranges_item_data in self.owner_ranges:
            owner_ranges_item = owner_ranges_item_data.to_dict()
            owner_ranges.append(owner_ranges_item)

        lifecycle = self.lifecycle.value

        lag = self.lag

        ready_watermark = self.ready_watermark

        failure_reason = self.failure_reason

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "generation": generation,
                "owner_ranges": owner_ranges,
                "lifecycle": lifecycle,
                "lag": lag,
                "ready_watermark": ready_watermark,
            }
        )
        if failure_reason is not UNSET:
            field_dict["failure_reason"] = failure_reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_index_owner_range import RelationalIndexOwnerRange

        d = dict(src_dict)
        generation = d.pop("generation")

        owner_ranges = []
        _owner_ranges = d.pop("owner_ranges")
        for owner_ranges_item_data in _owner_ranges:
            owner_ranges_item = RelationalIndexOwnerRange.from_dict(owner_ranges_item_data)

            owner_ranges.append(owner_ranges_item)

        lifecycle = RelationalIndexGenerationRecordLifecycle(d.pop("lifecycle"))

        lag = d.pop("lag")

        ready_watermark = d.pop("ready_watermark")

        failure_reason = d.pop("failure_reason", UNSET)

        relational_index_generation_record = cls(
            generation=generation,
            owner_ranges=owner_ranges,
            lifecycle=lifecycle,
            lag=lag,
            ready_watermark=ready_watermark,
            failure_reason=failure_reason,
        )

        return relational_index_generation_record
