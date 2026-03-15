from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ChunkerConfigFullTextIndex")


@_attrs_define
class ChunkerConfigFullTextIndex:
    """Configuration for full-text indexing of chunks in Bleve.
    When present (even if empty), chunks will be stored with :cft: suffix and indexed in Bleve's _chunks field.
    When absent, chunks use :c: suffix and are only used for vector embeddings.

    """

    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        chunker_config_full_text_index = cls()

        chunker_config_full_text_index.additional_properties = d
        return chunker_config_full_text_index

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
