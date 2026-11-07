from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.privacy_preview_documents_item import PrivacyPreviewDocumentsItem


T = TypeVar("T", bound="PrivacyPreview")


@_attrs_define
class PrivacyPreview:
    """
    Attributes:
        policy_version (str):
        sampled (int):
        complete_table_scan (bool):
        documents (list[PrivacyPreviewDocumentsItem]):
        external_processing (bool | Unset):
    """

    policy_version: str
    sampled: int
    complete_table_scan: bool
    documents: list[PrivacyPreviewDocumentsItem]
    external_processing: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        policy_version = self.policy_version

        sampled = self.sampled

        complete_table_scan = self.complete_table_scan

        documents = []
        for documents_item_data in self.documents:
            documents_item = documents_item_data.to_dict()
            documents.append(documents_item)

        external_processing = self.external_processing

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "policy_version": policy_version,
                "sampled": sampled,
                "complete_table_scan": complete_table_scan,
                "documents": documents,
            }
        )
        if external_processing is not UNSET:
            field_dict["external_processing"] = external_processing

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.privacy_preview_documents_item import PrivacyPreviewDocumentsItem

        d = dict(src_dict)
        policy_version = d.pop("policy_version")

        sampled = d.pop("sampled")

        complete_table_scan = d.pop("complete_table_scan")

        documents = []
        _documents = d.pop("documents")
        for documents_item_data in _documents:
            documents_item = PrivacyPreviewDocumentsItem.from_dict(documents_item_data)

            documents.append(documents_item)

        external_processing = d.pop("external_processing", UNSET)

        privacy_preview = cls(
            policy_version=policy_version,
            sampled=sampled,
            complete_table_scan=complete_table_scan,
            documents=documents,
            external_processing=external_processing,
        )

        privacy_preview.additional_properties = d
        return privacy_preview

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
