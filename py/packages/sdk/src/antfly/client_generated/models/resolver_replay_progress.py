from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.resolver_group_progress import ResolverGroupProgress


T = TypeVar("T", bound="ResolverReplayProgress")


@_attrs_define
class ResolverReplayProgress:
    """
    Attributes:
        applied (bool):
        shards (list[ResolverGroupProgress]):
    """

    applied: bool
    shards: list[ResolverGroupProgress]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        applied = self.applied

        shards = []
        for shards_item_data in self.shards:
            shards_item = shards_item_data.to_dict()
            shards.append(shards_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "applied": applied,
                "shards": shards,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_group_progress import ResolverGroupProgress

        d = dict(src_dict)
        applied = d.pop("applied")

        shards = []
        _shards = d.pop("shards")
        for shards_item_data in _shards:
            shards_item = ResolverGroupProgress.from_dict(shards_item_data)

            shards.append(shards_item)

        resolver_replay_progress = cls(
            applied=applied,
            shards=shards,
        )

        resolver_replay_progress.additional_properties = d
        return resolver_replay_progress

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
