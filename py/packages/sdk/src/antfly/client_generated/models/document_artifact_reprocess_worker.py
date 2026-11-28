from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_artifact_retry_backlog import DocumentArtifactRetryBacklog


T = TypeVar("T", bound="DocumentArtifactReprocessWorker")


@_attrs_define
class DocumentArtifactReprocessWorker:
    """Sample of the owning shard's enrichment worker, when present. The worker services multiple documents and producers.
    Sequence values use the shard's enrichment replay sequence space.

        Attributes:
            scanned_sequence (int): Replay scan watermark, which can run ahead of the contiguous applied watermark while
                requests wait.
            applied_sequence (int): Contiguous applied enrichment watermark.
            target_sequence (int): Latest enrichment replay watermark requested of the worker.
            retry_queue_full (bool): Whether retry debt is preventing the worker from admitting new source groups.
            retrying (bool): Whether the worker has retry work.
            failed (bool): Whether the shard's worker has failed.
            background_running (bool): Whether a background worker has started. Embedded callers can also drive replay
                inline.
            backlog (DocumentArtifactRetryBacklog | Unset): Shard-wide retry debt sampled from the replay worker.
            next_retry_at_ms (int | Unset): Unix timestamp in milliseconds for the worker's next scheduled retry, when
                available. This is shard-wide.
            last_error_code (str | Unset): Latest known worker error such as a publication wait. This is shard-wide.
            last_progress_at_ms (int | Unset): Unix timestamp of the last observed worker activity, when available. This is
                shard-wide.
    """

    scanned_sequence: int
    applied_sequence: int
    target_sequence: int
    retry_queue_full: bool
    retrying: bool
    failed: bool
    background_running: bool
    backlog: DocumentArtifactRetryBacklog | Unset = UNSET
    next_retry_at_ms: int | Unset = UNSET
    last_error_code: str | Unset = UNSET
    last_progress_at_ms: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        scanned_sequence = self.scanned_sequence

        applied_sequence = self.applied_sequence

        target_sequence = self.target_sequence

        retry_queue_full = self.retry_queue_full

        retrying = self.retrying

        failed = self.failed

        background_running = self.background_running

        backlog: dict[str, Any] | Unset = UNSET
        if not isinstance(self.backlog, Unset):
            backlog = self.backlog.to_dict()

        next_retry_at_ms = self.next_retry_at_ms

        last_error_code = self.last_error_code

        last_progress_at_ms = self.last_progress_at_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "scanned_sequence": scanned_sequence,
                "applied_sequence": applied_sequence,
                "target_sequence": target_sequence,
                "retry_queue_full": retry_queue_full,
                "retrying": retrying,
                "failed": failed,
                "background_running": background_running,
            }
        )
        if backlog is not UNSET:
            field_dict["backlog"] = backlog
        if next_retry_at_ms is not UNSET:
            field_dict["next_retry_at_ms"] = next_retry_at_ms
        if last_error_code is not UNSET:
            field_dict["last_error_code"] = last_error_code
        if last_progress_at_ms is not UNSET:
            field_dict["last_progress_at_ms"] = last_progress_at_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_artifact_retry_backlog import DocumentArtifactRetryBacklog

        d = dict(src_dict)
        scanned_sequence = d.pop("scanned_sequence")

        applied_sequence = d.pop("applied_sequence")

        target_sequence = d.pop("target_sequence")

        retry_queue_full = d.pop("retry_queue_full")

        retrying = d.pop("retrying")

        failed = d.pop("failed")

        background_running = d.pop("background_running")

        _backlog = d.pop("backlog", UNSET)
        backlog: DocumentArtifactRetryBacklog | Unset
        if isinstance(_backlog, Unset):
            backlog = UNSET
        else:
            backlog = DocumentArtifactRetryBacklog.from_dict(_backlog)

        next_retry_at_ms = d.pop("next_retry_at_ms", UNSET)

        last_error_code = d.pop("last_error_code", UNSET)

        last_progress_at_ms = d.pop("last_progress_at_ms", UNSET)

        document_artifact_reprocess_worker = cls(
            scanned_sequence=scanned_sequence,
            applied_sequence=applied_sequence,
            target_sequence=target_sequence,
            retry_queue_full=retry_queue_full,
            retrying=retrying,
            failed=failed,
            background_running=background_running,
            backlog=backlog,
            next_retry_at_ms=next_retry_at_ms,
            last_error_code=last_error_code,
            last_progress_at_ms=last_progress_at_ms,
        )

        document_artifact_reprocess_worker.additional_properties = d
        return document_artifact_reprocess_worker

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
