from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.document_artifact_manifest import DocumentArtifactManifest


T = TypeVar("T", bound="DocumentArtifactManifestList")


@_attrs_define
class DocumentArtifactManifestList:
    """Available derived document artifact manifests for a source document.

    Attributes:
        document_id (str): Stable identity of the source document.
        artifacts (list[DocumentArtifactManifest]):
    """

    document_id: str
    artifacts: list[DocumentArtifactManifest]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        document_id = self.document_id

        artifacts = []
        for artifacts_item_data in self.artifacts:
            artifacts_item = artifacts_item_data.to_dict()
            artifacts.append(artifacts_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "document_id": document_id,
                "artifacts": artifacts,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_artifact_manifest import DocumentArtifactManifest

        d = dict(src_dict)
        document_id = d.pop("document_id")

        artifacts = []
        _artifacts = d.pop("artifacts")
        for artifacts_item_data in _artifacts:
            artifacts_item = DocumentArtifactManifest.from_dict(artifacts_item_data)

            artifacts.append(artifacts_item)

        document_artifact_manifest_list = cls(
            document_id=document_id,
            artifacts=artifacts,
        )

        document_artifact_manifest_list.additional_properties = d
        return document_artifact_manifest_list

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
