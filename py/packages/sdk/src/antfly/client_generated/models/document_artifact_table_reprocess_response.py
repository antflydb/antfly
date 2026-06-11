from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_artifact_table_reprocess_response_reprocess import (
    DocumentArtifactTableReprocessResponseReprocess,
)
from ..models.document_artifact_table_reprocess_response_reprocess_status import (
    DocumentArtifactTableReprocessResponseReprocessStatus,
)
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_artifact_reprocess_failure import DocumentArtifactReprocessFailure
    from ..models.document_artifact_reprocess_shard_cursor import DocumentArtifactReprocessShardCursor


T = TypeVar("T", bound="DocumentArtifactTableReprocessResponse")


@_attrs_define
class DocumentArtifactTableReprocessResponse:
    """
    Attributes:
        reprocess (DocumentArtifactTableReprocessResponseReprocess): Indicates that reprocessing was accepted.
        reprocess_status (DocumentArtifactTableReprocessResponseReprocessStatus): Completion state for this bounded
            pass. `in_progress` means the caller should persist the returned cursor(s) and schedule another pass; `complete`
            means no continuation cursor remains.
        artifact_name (str): Name of the derived artifact that was reprocessed.
        scanned (int): Number of source rows scanned by this bounded pass.
        reprocessed (int): Number of source rows whose artifact was reprocessed.
        skipped (int): Number of scanned source rows that no longer had a reprocessable source document.
        failed (int): Number of scanned source rows that failed before recording a normal artifact manifest.
        limit (int): Effective scan limit used by the bounded pass.
        pending_shards (int): Number of shard-local continuations still pending after this pass. For single-shard
            callers this is 1 when only `next_key` remains and 0 when complete.
        failures (list[DocumentArtifactReprocessFailure]):
        shard_cursors (list[DocumentArtifactReprocessShardCursor]): Per-shard continuation cursors for distributed
            repairs. Durable background repair jobs should persist and resume these independently instead of collapsing
            progress into a single global cursor.
        next_key (None | str | Unset): Source key cursor for the next bounded pass, when more rows may remain.
    """

    reprocess: DocumentArtifactTableReprocessResponseReprocess
    reprocess_status: DocumentArtifactTableReprocessResponseReprocessStatus
    artifact_name: str
    scanned: int
    reprocessed: int
    skipped: int
    failed: int
    limit: int
    pending_shards: int
    failures: list[DocumentArtifactReprocessFailure]
    shard_cursors: list[DocumentArtifactReprocessShardCursor]
    next_key: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        reprocess = self.reprocess.value

        reprocess_status = self.reprocess_status.value

        artifact_name = self.artifact_name

        scanned = self.scanned

        reprocessed = self.reprocessed

        skipped = self.skipped

        failed = self.failed

        limit = self.limit

        pending_shards = self.pending_shards

        failures = []
        for failures_item_data in self.failures:
            failures_item = failures_item_data.to_dict()
            failures.append(failures_item)

        shard_cursors = []
        for shard_cursors_item_data in self.shard_cursors:
            shard_cursors_item = shard_cursors_item_data.to_dict()
            shard_cursors.append(shard_cursors_item)

        next_key: None | str | Unset
        if isinstance(self.next_key, Unset):
            next_key = UNSET
        else:
            next_key = self.next_key

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "reprocess": reprocess,
                "reprocess_status": reprocess_status,
                "artifact_name": artifact_name,
                "scanned": scanned,
                "reprocessed": reprocessed,
                "skipped": skipped,
                "failed": failed,
                "limit": limit,
                "pending_shards": pending_shards,
                "failures": failures,
                "shard_cursors": shard_cursors,
            }
        )
        if next_key is not UNSET:
            field_dict["next_key"] = next_key

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_artifact_reprocess_failure import DocumentArtifactReprocessFailure
        from ..models.document_artifact_reprocess_shard_cursor import DocumentArtifactReprocessShardCursor

        d = dict(src_dict)
        reprocess = DocumentArtifactTableReprocessResponseReprocess(d.pop("reprocess"))

        reprocess_status = DocumentArtifactTableReprocessResponseReprocessStatus(d.pop("reprocess_status"))

        artifact_name = d.pop("artifact_name")

        scanned = d.pop("scanned")

        reprocessed = d.pop("reprocessed")

        skipped = d.pop("skipped")

        failed = d.pop("failed")

        limit = d.pop("limit")

        pending_shards = d.pop("pending_shards")

        failures = []
        _failures = d.pop("failures")
        for failures_item_data in _failures:
            failures_item = DocumentArtifactReprocessFailure.from_dict(failures_item_data)

            failures.append(failures_item)

        shard_cursors = []
        _shard_cursors = d.pop("shard_cursors")
        for shard_cursors_item_data in _shard_cursors:
            shard_cursors_item = DocumentArtifactReprocessShardCursor.from_dict(shard_cursors_item_data)

            shard_cursors.append(shard_cursors_item)

        def _parse_next_key(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        next_key = _parse_next_key(d.pop("next_key", UNSET))

        document_artifact_table_reprocess_response = cls(
            reprocess=reprocess,
            reprocess_status=reprocess_status,
            artifact_name=artifact_name,
            scanned=scanned,
            reprocessed=reprocessed,
            skipped=skipped,
            failed=failed,
            limit=limit,
            pending_shards=pending_shards,
            failures=failures,
            shard_cursors=shard_cursors,
            next_key=next_key,
        )

        document_artifact_table_reprocess_response.additional_properties = d
        return document_artifact_table_reprocess_response

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
