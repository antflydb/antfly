from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.resolver_replay_stage import ResolverReplayStage


T = TypeVar("T", bound="ResolverGroupProgress")


@_attrs_define
class ResolverGroupProgress:
    """
    Attributes:
        group_id (int):
        available (bool):
        fresh (bool):
        curation_revision (int):
        config_generation (int):
        effective_generation (int):
        backfill_pending (bool):
        resolution (ResolverReplayStage):
        promotion (ResolverReplayStage):
    """

    group_id: int
    available: bool
    fresh: bool
    curation_revision: int
    config_generation: int
    effective_generation: int
    backfill_pending: bool
    resolution: ResolverReplayStage
    promotion: ResolverReplayStage
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        group_id = self.group_id

        available = self.available

        fresh = self.fresh

        curation_revision = self.curation_revision

        config_generation = self.config_generation

        effective_generation = self.effective_generation

        backfill_pending = self.backfill_pending

        resolution = self.resolution.to_dict()

        promotion = self.promotion.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "group_id": group_id,
                "available": available,
                "fresh": fresh,
                "curation_revision": curation_revision,
                "config_generation": config_generation,
                "effective_generation": effective_generation,
                "backfill_pending": backfill_pending,
                "resolution": resolution,
                "promotion": promotion,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_replay_stage import ResolverReplayStage

        d = dict(src_dict)
        group_id = d.pop("group_id")

        available = d.pop("available")

        fresh = d.pop("fresh")

        curation_revision = d.pop("curation_revision")

        config_generation = d.pop("config_generation")

        effective_generation = d.pop("effective_generation")

        backfill_pending = d.pop("backfill_pending")

        resolution = ResolverReplayStage.from_dict(d.pop("resolution"))

        promotion = ResolverReplayStage.from_dict(d.pop("promotion"))

        resolver_group_progress = cls(
            group_id=group_id,
            available=available,
            fresh=fresh,
            curation_revision=curation_revision,
            config_generation=config_generation,
            effective_generation=effective_generation,
            backfill_pending=backfill_pending,
            resolution=resolution,
            promotion=promotion,
        )

        resolver_group_progress.additional_properties = d
        return resolver_group_progress

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
