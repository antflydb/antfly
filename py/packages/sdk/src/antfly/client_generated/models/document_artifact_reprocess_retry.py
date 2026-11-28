from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_artifact_reprocess_retry_reason import DocumentArtifactReprocessRetryReason
from ..models.document_artifact_reprocess_retry_stage import DocumentArtifactReprocessRetryStage
from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentArtifactReprocessRetry")


@_attrs_define
class DocumentArtifactReprocessRetry:
    """Most recent durable deferral for this accepted extraction attempt, retained independently of publication status.
    This is a historical observation and does not imply the attempt is still waiting.

        Attributes:
            reason (DocumentArtifactReprocessRetryReason):
            retry_at_ms (int): Unix timestamp in milliseconds before which this attempt is deferred.
            stage (DocumentArtifactReprocessRetryStage | Unset): Enrichment stage identified by the retry outcome, when
                available. Enrichment is used when the failure does not identify a narrower stage.
            error_code (str | Unset): Stable error identifier associated with the deferral, when available.
    """

    reason: DocumentArtifactReprocessRetryReason
    retry_at_ms: int
    stage: DocumentArtifactReprocessRetryStage | Unset = UNSET
    error_code: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        reason = self.reason.value

        retry_at_ms = self.retry_at_ms

        stage: str | Unset = UNSET
        if not isinstance(self.stage, Unset):
            stage = self.stage.value

        error_code = self.error_code

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "reason": reason,
                "retry_at_ms": retry_at_ms,
            }
        )
        if stage is not UNSET:
            field_dict["stage"] = stage
        if error_code is not UNSET:
            field_dict["error_code"] = error_code

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        reason = DocumentArtifactReprocessRetryReason(d.pop("reason"))

        retry_at_ms = d.pop("retry_at_ms")

        _stage = d.pop("stage", UNSET)
        stage: DocumentArtifactReprocessRetryStage | Unset
        if isinstance(_stage, Unset):
            stage = UNSET
        else:
            stage = DocumentArtifactReprocessRetryStage(_stage)

        error_code = d.pop("error_code", UNSET)

        document_artifact_reprocess_retry = cls(
            reason=reason,
            retry_at_ms=retry_at_ms,
            stage=stage,
            error_code=error_code,
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
