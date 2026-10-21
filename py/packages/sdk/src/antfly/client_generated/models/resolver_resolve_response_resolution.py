from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resolver_resolve_response_resolution_resolution_work import (
        ResolverResolveResponseResolutionResolutionWork,
    )
    from ..models.resolver_resolved_entity import ResolverResolvedEntity


T = TypeVar("T", bound="ResolverResolveResponseResolution")


@_attrs_define
class ResolverResolveResponseResolution:
    """
    Attributes:
        config_generation (int):
        entities (list[ResolverResolvedEntity]):
        skipped_mentions (int | Unset):
        field_resolution_work (ResolverResolveResponseResolutionResolutionWork | Unset):
    """

    config_generation: int
    entities: list[ResolverResolvedEntity]
    skipped_mentions: int | Unset = UNSET
    field_resolution_work: ResolverResolveResponseResolutionResolutionWork | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        config_generation = self.config_generation

        entities = []
        for entities_item_data in self.entities:
            entities_item = entities_item_data.to_dict()
            entities.append(entities_item)

        skipped_mentions = self.skipped_mentions

        field_resolution_work: dict[str, Any] | Unset = UNSET
        if not isinstance(self.field_resolution_work, Unset):
            field_resolution_work = self.field_resolution_work.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "config_generation": config_generation,
                "entities": entities,
            }
        )
        if skipped_mentions is not UNSET:
            field_dict["skipped_mentions"] = skipped_mentions
        if field_resolution_work is not UNSET:
            field_dict["_resolution_work"] = field_resolution_work

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_resolve_response_resolution_resolution_work import (
            ResolverResolveResponseResolutionResolutionWork,
        )
        from ..models.resolver_resolved_entity import ResolverResolvedEntity

        d = dict(src_dict)
        config_generation = d.pop("config_generation")

        entities = []
        _entities = d.pop("entities")
        for entities_item_data in _entities:
            entities_item = ResolverResolvedEntity.from_dict(entities_item_data)

            entities.append(entities_item)

        skipped_mentions = d.pop("skipped_mentions", UNSET)

        _field_resolution_work = d.pop("_resolution_work", UNSET)
        field_resolution_work: ResolverResolveResponseResolutionResolutionWork | Unset
        if isinstance(_field_resolution_work, Unset):
            field_resolution_work = UNSET
        else:
            field_resolution_work = ResolverResolveResponseResolutionResolutionWork.from_dict(_field_resolution_work)

        resolver_resolve_response_resolution = cls(
            config_generation=config_generation,
            entities=entities,
            skipped_mentions=skipped_mentions,
            field_resolution_work=field_resolution_work,
        )

        resolver_resolve_response_resolution.additional_properties = d
        return resolver_resolve_response_resolution

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
