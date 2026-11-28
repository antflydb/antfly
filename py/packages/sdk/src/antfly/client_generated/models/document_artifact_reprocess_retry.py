from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_artifact_reprocess_retry_reason import DocumentArtifactReprocessRetryReason

T = TypeVar("T", bound="DocumentArtifactReprocessRetry")


@_attrs_define
class DocumentArtifactReprocessRetry:
    """Most recent durable deferral for this accepted extraction attempt. The deadline can be in the past if replay has
    since resumed.

        Attributes:
            reason (DocumentArtifactReprocessRetryReason):
            retry_at_ms (int): Unix timestamp in milliseconds before which this attempt is deferred.
    """

    reason: DocumentArtifactReprocessRetryReason
    retry_at_ms: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        reason = self.reason.value

        retry_at_ms = self.retry_at_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "reason": reason,
                "retry_at_ms": retry_at_ms,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        reason = DocumentArtifactReprocessRetryReason(d.pop("reason"))

        retry_at_ms = d.pop("retry_at_ms")

        document_artifact_reprocess_retry = cls(
            reason=reason,
            retry_at_ms=retry_at_ms,
        )

        document_artifact_reprocess_retry.additional_properties = d
        return document_artifact_reprocess_retry

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
