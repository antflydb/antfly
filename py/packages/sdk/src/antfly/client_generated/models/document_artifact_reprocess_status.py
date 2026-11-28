from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_artifact_reprocess_status_status import DocumentArtifactReprocessStatusStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_artifact_reprocess_retry import DocumentArtifactReprocessRetry
    from ..models.document_artifact_reprocess_worker import DocumentArtifactReprocessWorker


T = TypeVar("T", bound="DocumentArtifactReprocessStatus")


@_attrs_define
class DocumentArtifactReprocessStatus:
    """Latest explicit native document-extraction reprocess intent for this artifact. Present on single-artifact inspection
    when an intent exists. When the first publication is pending, the summary has generation zero and no raw manifest.

        Attributes:
            requested_generation (int): Target generation recorded by the accepted request.
            status (DocumentArtifactReprocessStatusStatus): Published has a manifest receipt for the accepted request and
                can include extraction failures. Pending has no matching publication. Superseded no longer matches the
                configured producer. Unverified is a legacy intent whose target generation was reached without a publication
                receipt.
            accepted_sequence (int | Unset): Replay sequence committed with the request in the accepting shard, when
                available.
            worker (DocumentArtifactReprocessWorker | Unset): Sample of the owning shard's enrichment worker, when present.
                The worker services multiple documents and producers. Sequence values use the shard's enrichment replay sequence
                space.
            last_retry (DocumentArtifactReprocessRetry | Unset): Most recent durable deferral for this accepted extraction
                attempt. The deadline can be in the past if replay has since resumed.
    """

    requested_generation: int
    status: DocumentArtifactReprocessStatusStatus
    accepted_sequence: int | Unset = UNSET
    worker: DocumentArtifactReprocessWorker | Unset = UNSET
    last_retry: DocumentArtifactReprocessRetry | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        requested_generation = self.requested_generation

        status = self.status.value

        accepted_sequence = self.accepted_sequence

        worker: dict[str, Any] | Unset = UNSET
        if not isinstance(self.worker, Unset):
            worker = self.worker.to_dict()

        last_retry: dict[str, Any] | Unset = UNSET
        if not isinstance(self.last_retry, Unset):
            last_retry = self.last_retry.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "requested_generation": requested_generation,
                "status": status,
            }
        )
        if accepted_sequence is not UNSET:
            field_dict["accepted_sequence"] = accepted_sequence
        if worker is not UNSET:
            field_dict["worker"] = worker
        if last_retry is not UNSET:
            field_dict["last_retry"] = last_retry

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_artifact_reprocess_retry import DocumentArtifactReprocessRetry
        from ..models.document_artifact_reprocess_worker import DocumentArtifactReprocessWorker

        d = dict(src_dict)
        requested_generation = d.pop("requested_generation")

        status = DocumentArtifactReprocessStatusStatus(d.pop("status"))

        accepted_sequence = d.pop("accepted_sequence", UNSET)

        _worker = d.pop("worker", UNSET)
        worker: DocumentArtifactReprocessWorker | Unset
        if isinstance(_worker, Unset):
            worker = UNSET
        else:
            worker = DocumentArtifactReprocessWorker.from_dict(_worker)

        _last_retry = d.pop("last_retry", UNSET)
        last_retry: DocumentArtifactReprocessRetry | Unset
        if isinstance(_last_retry, Unset):
            last_retry = UNSET
        else:
            last_retry = DocumentArtifactReprocessRetry.from_dict(_last_retry)

        document_artifact_reprocess_status = cls(
            requested_generation=requested_generation,
            status=status,
            accepted_sequence=accepted_sequence,
            worker=worker,
            last_retry=last_retry,
        )

        document_artifact_reprocess_status.additional_properties = d
        return document_artifact_reprocess_status

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
