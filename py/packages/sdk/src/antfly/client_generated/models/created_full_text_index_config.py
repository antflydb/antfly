from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.full_text_artifact_index_source import FullTextArtifactIndexSource


T = TypeVar("T", bound="CreatedFullTextIndexConfig")


@_attrs_define
class CreatedFullTextIndexConfig:
    """Canonical full-text configuration returned after creation. Single-source alternative request forms are represented
    through sources.

        Attributes:
            sources (list[FullTextArtifactIndexSource] | Unset):
            mem_only (bool | Unset):
            field (str | Unset):
    """

    sources: list[FullTextArtifactIndexSource] | Unset = UNSET
    mem_only: bool | Unset = UNSET
    field: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        sources: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.sources, Unset):
            sources = []
            for sources_item_data in self.sources:
                sources_item = sources_item_data.to_dict()
                sources.append(sources_item)

        mem_only = self.mem_only

        field = self.field

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if sources is not UNSET:
            field_dict["sources"] = sources
        if mem_only is not UNSET:
            field_dict["mem_only"] = mem_only
        if field is not UNSET:
            field_dict["field"] = field

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.full_text_artifact_index_source import FullTextArtifactIndexSource

        d = dict(src_dict)
        _sources = d.pop("sources", UNSET)
        sources: list[FullTextArtifactIndexSource] | Unset = UNSET
        if _sources is not UNSET:
            sources = []
            for sources_item_data in _sources:
                sources_item = FullTextArtifactIndexSource.from_dict(sources_item_data)

                sources.append(sources_item)

        mem_only = d.pop("mem_only", UNSET)

        field = d.pop("field", UNSET)

        created_full_text_index_config = cls(
            sources=sources,
            mem_only=mem_only,
            field=field,
        )

        created_full_text_index_config.additional_properties = d
        return created_full_text_index_config

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
