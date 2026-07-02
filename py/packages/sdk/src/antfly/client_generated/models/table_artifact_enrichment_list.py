from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.enrichment_config import EnrichmentConfig


T = TypeVar("T", bound="TableArtifactEnrichmentList")


@_attrs_define
class TableArtifactEnrichmentList:
    """Table-level generated artifact enrichments configured for a table.

    Attributes:
        table_name (str): Table containing the configured artifact enrichments.
        artifacts (list[EnrichmentConfig]):
    """

    table_name: str
    artifacts: list[EnrichmentConfig]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table_name = self.table_name

        artifacts = []
        for artifacts_item_data in self.artifacts:
            artifacts_item = artifacts_item_data.to_dict()
            artifacts.append(artifacts_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table_name": table_name,
                "artifacts": artifacts,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.enrichment_config import EnrichmentConfig

        d = dict(src_dict)
        table_name = d.pop("table_name")

        artifacts = []
        _artifacts = d.pop("artifacts")
        for artifacts_item_data in _artifacts:
            artifacts_item = EnrichmentConfig.from_dict(artifacts_item_data)

            artifacts.append(artifacts_item)

        table_artifact_enrichment_list = cls(
            table_name=table_name,
            artifacts=artifacts,
        )

        table_artifact_enrichment_list.additional_properties = d
        return table_artifact_enrichment_list

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
