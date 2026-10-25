from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.resolver_replay_progress import ResolverReplayProgress


T = TypeVar("T", bound="EntityConsumerProgressPageResultsItem")


@_attrs_define
class EntityConsumerProgressPageResultsItem:
    """
    Attributes:
        source_table (str):
        resolver (str):
        key_template (str):
        revision (int):
        applied (bool):
        progress (ResolverReplayProgress):
    """

    source_table: str
    resolver: str
    key_template: str
    revision: int
    applied: bool
    progress: ResolverReplayProgress
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        source_table = self.source_table

        resolver = self.resolver

        key_template = self.key_template

        revision = self.revision

        applied = self.applied

        progress = self.progress.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "source_table": source_table,
                "resolver": resolver,
                "key_template": key_template,
                "revision": revision,
                "applied": applied,
                "progress": progress,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_replay_progress import ResolverReplayProgress

        d = dict(src_dict)
        source_table = d.pop("source_table")

        resolver = d.pop("resolver")

        key_template = d.pop("key_template")

        revision = d.pop("revision")

        applied = d.pop("applied")

        progress = ResolverReplayProgress.from_dict(d.pop("progress"))

        entity_consumer_progress_page_results_item = cls(
            source_table=source_table,
            resolver=resolver,
            key_template=key_template,
            revision=revision,
            applied=applied,
            progress=progress,
        )

        entity_consumer_progress_page_results_item.additional_properties = d
        return entity_consumer_progress_page_results_item

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
