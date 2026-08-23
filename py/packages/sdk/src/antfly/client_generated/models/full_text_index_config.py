from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="FullTextIndexConfig")


@_attrs_define
class FullTextIndexConfig:
    """
    Attributes:
        mem_only (bool | Unset): Whether to use memory-only storage
        field (str | Unset): Document field indexed as text. Omit for the table's default full-document text index.
        artifact_name (str | Unset): Generated artifact stream indexed as text. Use with matching inline enrichments.
    """

    mem_only: bool | Unset = UNSET
    field: str | Unset = UNSET
    artifact_name: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        mem_only = self.mem_only

        field = self.field

        artifact_name = self.artifact_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if mem_only is not UNSET:
            field_dict["mem_only"] = mem_only
        if field is not UNSET:
            field_dict["field"] = field
        if artifact_name is not UNSET:
            field_dict["artifact_name"] = artifact_name

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        mem_only = d.pop("mem_only", UNSET)

        field = d.pop("field", UNSET)

        artifact_name = d.pop("artifact_name", UNSET)

        full_text_index_config = cls(
            mem_only=mem_only,
            field=field,
            artifact_name=artifact_name,
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
