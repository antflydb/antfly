from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_artifact_reprocess_status_status import DocumentArtifactReprocessStatusStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_artifact_reprocess_worker import DocumentArtifactReprocessWorker


T = TypeVar("T", bound="DocumentArtifactReprocessStatus")


@_attrs_define
class DocumentArtifactReprocessStatus:
    """Latest explicit native document-extraction reprocess intent for this artifact. Present on single-artifact inspection
    when an intent exists. When the first publication is pending, the summary has generation zero and no raw manifest.

        Attributes:
            requested_generation (int): Target generation recorded by the accepted request.
            status (DocumentArtifactReprocessStatusStatus): Pending is an unfulfilled intent. Published means the target
                generation is visible and can itself contain extraction failures. Superseded means the intent no longer matches
                the configured producer.
            worker (DocumentArtifactReprocessWorker | Unset): Sample of the owning shard's enrichment worker, when present.
                The worker services multiple documents and producers. Sequence values use the shard's enrichment replay sequence
                space.
    """

    requested_generation: int
    status: DocumentArtifactReprocessStatusStatus
    worker: DocumentArtifactReprocessWorker | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        requested_generation = self.requested_generation

        status = self.status.value

        worker: dict[str, Any] | Unset = UNSET
        if not isinstance(self.worker, Unset):
            worker = self.worker.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "requested_generation": requested_generation,
                "status": status,
            }
        )
        if worker is not UNSET:
            field_dict["worker"] = worker

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_artifact_reprocess_worker import DocumentArtifactReprocessWorker

        d = dict(src_dict)
        requested_generation = d.pop("requested_generation")

        status = DocumentArtifactReprocessStatusStatus(d.pop("status"))

        _worker = d.pop("worker", UNSET)
        worker: DocumentArtifactReprocessWorker | Unset
        if isinstance(_worker, Unset):
            worker = UNSET
        else:
            worker = DocumentArtifactReprocessWorker.from_dict(_worker)

        document_artifact_reprocess_status = cls(
            requested_generation=requested_generation,
            status=status,
            worker=worker,
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
