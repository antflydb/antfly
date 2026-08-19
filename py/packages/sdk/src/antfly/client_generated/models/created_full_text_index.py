from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.created_full_text_index_type import CreatedFullTextIndexType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.enrichment_config import EnrichmentConfig


T = TypeVar("T", bound="CreatedFullTextIndex")


@_attrs_define
class CreatedFullTextIndex:
    """Normalized effective full-text index configuration returned after creation.

    Attributes:
        name (str): Name of the created index
        type_ (CreatedFullTextIndexType):
        description (str | Unset): Optional description of the index and its purpose
        version (int | Unset): Version of the index implementation. Defaults to 0. Default: 0.
        enrichments (list[EnrichmentConfig] | Unset): Normalized inline managed enrichment definitions required by this
            index.
        mem_only (bool | Unset): Whether to use memory-only storage
        field (str | Unset): Document field indexed as text. Omit for the table's default full-document text index.
        artifact_name (str | Unset): Generated artifact stream indexed as text. Use with matching inline enrichments.
    """

    name: str
    type_: CreatedFullTextIndexType
    description: str | Unset = UNSET
    version: int | Unset = 0
    enrichments: list[EnrichmentConfig] | Unset = UNSET
    mem_only: bool | Unset = UNSET
    field: str | Unset = UNSET
    artifact_name: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        type_ = self.type_.value

        description = self.description

        version = self.version

        enrichments: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.enrichments, Unset):
            enrichments = []
            for enrichments_item_data in self.enrichments:
                enrichments_item = enrichments_item_data.to_dict()
                enrichments.append(enrichments_item)

        mem_only = self.mem_only

        field = self.field

        artifact_name = self.artifact_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "type": type_,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if version is not UNSET:
            field_dict["version"] = version
        if enrichments is not UNSET:
            field_dict["enrichments"] = enrichments
        if mem_only is not UNSET:
            field_dict["mem_only"] = mem_only
        if field is not UNSET:
            field_dict["field"] = field
        if artifact_name is not UNSET:
            field_dict["artifact_name"] = artifact_name

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.enrichment_config import EnrichmentConfig

        d = dict(src_dict)
        name = d.pop("name")

        type_ = CreatedFullTextIndexType(d.pop("type"))

        description = d.pop("description", UNSET)

        version = d.pop("version", UNSET)

        _enrichments = d.pop("enrichments", UNSET)
        enrichments: list[EnrichmentConfig] | Unset = UNSET
        if _enrichments is not UNSET:
            enrichments = []
            for enrichments_item_data in _enrichments:
                enrichments_item = EnrichmentConfig.from_dict(enrichments_item_data)

                enrichments.append(enrichments_item)

        mem_only = d.pop("mem_only", UNSET)

        field = d.pop("field", UNSET)

        artifact_name = d.pop("artifact_name", UNSET)

        created_full_text_index = cls(
            name=name,
            type_=type_,
            description=description,
            version=version,
            enrichments=enrichments,
            mem_only=mem_only,
            field=field,
            artifact_name=artifact_name,
        )

        created_full_text_index.additional_properties = d
        return created_full_text_index

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
