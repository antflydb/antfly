from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.artifact_index_source import ArtifactIndexSource


T = TypeVar("T", bound="FullTextIndexConfig")


@_attrs_define
class FullTextIndexConfig:
    """
    Attributes:
        sources (list[ArtifactIndexSource] | Unset): Chunk or textual asset artifact streams indexed together. Every
            artifact record is an independent full-text member.
        mem_only (bool | Unset): Whether to use memory-only storage
    """

    sources: list[ArtifactIndexSource] | Unset = UNSET
    mem_only: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        sources: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.sources, Unset):
            sources = []
            for sources_item_data in self.sources:
                sources_item = sources_item_data.to_dict()
                sources.append(sources_item)

        mem_only = self.mem_only

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if sources is not UNSET:
            field_dict["sources"] = sources
        if mem_only is not UNSET:
            field_dict["mem_only"] = mem_only

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.artifact_index_source import ArtifactIndexSource

        d = dict(src_dict)
        _sources = d.pop("sources", UNSET)
        sources: list[ArtifactIndexSource] | Unset = UNSET
        if _sources is not UNSET:
            sources = []
            for sources_item_data in _sources:
                sources_item = ArtifactIndexSource.from_dict(sources_item_data)

                sources.append(sources_item)

        mem_only = d.pop("mem_only", UNSET)

        full_text_index_config = cls(
            sources=sources,
            mem_only=mem_only,
        )

        full_text_index_config.additional_properties = d
        return full_text_index_config

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
