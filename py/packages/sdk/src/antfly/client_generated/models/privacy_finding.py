from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="PrivacyFinding")


@_attrs_define
class PrivacyFinding:
    """Half-open UTF-8 byte offsets into the immutable source field. Matched values are not persisted in findings.
    Confidence is omitted when unavailable.

        Attributes:
            document (str):
            field (str):
            category (str):
            start (int):
            end (int):
            policy_version (str):
            confidence (float | Unset):
            source_fingerprint (str | Unset):
            producer_revision (str | Unset):
    """

    document: str
    field: str
    category: str
    start: int
    end: int
    policy_version: str
    confidence: float | Unset = UNSET
    source_fingerprint: str | Unset = UNSET
    producer_revision: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        document = self.document

        field = self.field

        category = self.category

        start = self.start

        end = self.end

        policy_version = self.policy_version

        confidence = self.confidence

        source_fingerprint = self.source_fingerprint

        producer_revision = self.producer_revision

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "document": document,
                "field": field,
                "category": category,
                "start": start,
                "end": end,
                "policy_version": policy_version,
            }
        )
        if confidence is not UNSET:
            field_dict["confidence"] = confidence
        if source_fingerprint is not UNSET:
            field_dict["source_fingerprint"] = source_fingerprint
        if producer_revision is not UNSET:
            field_dict["producer_revision"] = producer_revision

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        document = d.pop("document")

        field = d.pop("field")

        category = d.pop("category")

        start = d.pop("start")

        end = d.pop("end")

        policy_version = d.pop("policy_version")

        confidence = d.pop("confidence", UNSET)

        source_fingerprint = d.pop("source_fingerprint", UNSET)

        producer_revision = d.pop("producer_revision", UNSET)

        privacy_finding = cls(
            document=document,
            field=field,
            category=category,
            start=start,
            end=end,
            policy_version=policy_version,
            confidence=confidence,
            source_fingerprint=source_fingerprint,
            producer_revision=producer_revision,
        )

        privacy_finding.additional_properties = d
        return privacy_finding

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
