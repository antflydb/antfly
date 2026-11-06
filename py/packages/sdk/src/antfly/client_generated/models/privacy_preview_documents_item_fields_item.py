from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.privacy_finding import PrivacyFinding


T = TypeVar("T", bound="PrivacyPreviewDocumentsItemFieldsItem")


@_attrs_define
class PrivacyPreviewDocumentsItemFieldsItem:
    """
    Attributes:
        field (str):
        original (str):
        text (str):
        findings (list[PrivacyFinding]):
    """

    field: str
    original: str
    text: str
    findings: list[PrivacyFinding]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        original = self.original

        text = self.text

        findings = []
        for findings_item_data in self.findings:
            findings_item = findings_item_data.to_dict()
            findings.append(findings_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "field": field,
                "original": original,
                "text": text,
                "findings": findings,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.privacy_finding import PrivacyFinding

        d = dict(src_dict)
        field = d.pop("field")

        original = d.pop("original")

        text = d.pop("text")

        findings = []
        _findings = d.pop("findings")
        for findings_item_data in _findings:
            findings_item = PrivacyFinding.from_dict(findings_item_data)

            findings.append(findings_item)

        privacy_preview_documents_item_fields_item = cls(
            field=field,
            original=original,
            text=text,
            findings=findings,
        )

        privacy_preview_documents_item_fields_item.additional_properties = d
        return privacy_preview_documents_item_fields_item

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
