from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_artifact_reprocess_job_phase import DocumentArtifactReprocessJobPhase
from ..models.document_artifact_reprocess_job_reprocess_status import DocumentArtifactReprocessJobReprocessStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_artifact_reprocess_failure import DocumentArtifactReprocessFailure
    from ..models.document_artifact_reprocess_shard_cursor import DocumentArtifactReprocessShardCursor


T = TypeVar("T", bound="DocumentArtifactReprocessJob")


@_attrs_define
class DocumentArtifactReprocessJob:
    """
    Attributes:
        job_id (int): Server-assigned durable repair job identifier.
        attempt_id (int): Monotonic execution attempt token for the current running pass.
        table_name (str): Table containing the source documents being repaired.
        artifact_name (str): Name of the derived artifact being repaired.
        phase (DocumentArtifactReprocessJobPhase): Lifecycle phase of the repair job.
        reprocess_status (DocumentArtifactReprocessJobReprocessStatus): User-facing completion status derived from the
            phase and remaining cursors.
        from_key (str): Original exclusive lower bound for the job.
        to_key (str): Original inclusive upper bound for the job, or empty for the end of the table/range.
        limit (int): Current per-shard bounded pass limit.
        scanned (int): Cumulative source rows scanned by completed passes.
        reprocessed (int): Cumulative source rows whose artifact was reprocessed.
        skipped (int): Cumulative source rows skipped by completed passes.
        failed (int): Cumulative source rows that failed during completed passes.
        pending_shards (int): Number of shard-local continuations still pending.
        failures (list[DocumentArtifactReprocessFailure]): Failures from the most recent completed pass.
        shard_cursors (list[DocumentArtifactReprocessShardCursor]): Per-shard continuation cursors to resume on the next
            advance operation.
        cancel_requested (bool): Whether cancellation has been requested for a running pass. Running passes finish at a
            bounded reprocess boundary before the job transitions to cancelled.
        created_at_millis (int): Unix epoch milliseconds when the job was created.
        last_updated_at_millis (int): Unix epoch milliseconds when the job was last updated.
        expires_at_millis (int): Unix epoch milliseconds after which the retained job status may be removed.
        next_key (None | str | Unset): Single-shard continuation key when no shard cursors are present.
        last_error (None | str | Unset): Last terminal or transient job error, when available.
    """

    job_id: int
    attempt_id: int
    table_name: str
    artifact_name: str
    phase: DocumentArtifactReprocessJobPhase
    reprocess_status: DocumentArtifactReprocessJobReprocessStatus
    from_key: str
    to_key: str
    limit: int
    scanned: int
    reprocessed: int
    skipped: int
    failed: int
    pending_shards: int
    failures: list[DocumentArtifactReprocessFailure]
    shard_cursors: list[DocumentArtifactReprocessShardCursor]
    cancel_requested: bool
    created_at_millis: int
    last_updated_at_millis: int
    expires_at_millis: int
    next_key: None | str | Unset = UNSET
    last_error: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        job_id = self.job_id

        attempt_id = self.attempt_id

        table_name = self.table_name

        artifact_name = self.artifact_name

        phase = self.phase.value

        reprocess_status = self.reprocess_status.value

        from_key = self.from_key

        to_key = self.to_key

        limit = self.limit

        scanned = self.scanned

        reprocessed = self.reprocessed

        skipped = self.skipped

        failed = self.failed

        pending_shards = self.pending_shards

        failures = []
        for failures_item_data in self.failures:
            failures_item = failures_item_data.to_dict()
            failures.append(failures_item)

        shard_cursors = []
        for shard_cursors_item_data in self.shard_cursors:
            shard_cursors_item = shard_cursors_item_data.to_dict()
            shard_cursors.append(shard_cursors_item)

        cancel_requested = self.cancel_requested

        created_at_millis = self.created_at_millis

        last_updated_at_millis = self.last_updated_at_millis

        expires_at_millis = self.expires_at_millis

        next_key: None | str | Unset
        if isinstance(self.next_key, Unset):
            next_key = UNSET
        else:
            next_key = self.next_key

        last_error: None | str | Unset
        if isinstance(self.last_error, Unset):
            last_error = UNSET
        else:
            last_error = self.last_error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "job_id": job_id,
                "attempt_id": attempt_id,
                "table_name": table_name,
                "artifact_name": artifact_name,
                "phase": phase,
                "reprocess_status": reprocess_status,
                "from_key": from_key,
                "to_key": to_key,
                "limit": limit,
                "scanned": scanned,
                "reprocessed": reprocessed,
                "skipped": skipped,
                "failed": failed,
                "pending_shards": pending_shards,
                "failures": failures,
                "shard_cursors": shard_cursors,
                "cancel_requested": cancel_requested,
                "created_at_millis": created_at_millis,
                "last_updated_at_millis": last_updated_at_millis,
                "expires_at_millis": expires_at_millis,
            }
        )
        if next_key is not UNSET:
            field_dict["next_key"] = next_key
        if last_error is not UNSET:
            field_dict["last_error"] = last_error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_artifact_reprocess_failure import DocumentArtifactReprocessFailure
        from ..models.document_artifact_reprocess_shard_cursor import DocumentArtifactReprocessShardCursor

        d = dict(src_dict)
        job_id = d.pop("job_id")

        attempt_id = d.pop("attempt_id")

        table_name = d.pop("table_name")

        artifact_name = d.pop("artifact_name")

        phase = DocumentArtifactReprocessJobPhase(d.pop("phase"))

        reprocess_status = DocumentArtifactReprocessJobReprocessStatus(d.pop("reprocess_status"))

        from_key = d.pop("from_key")

        to_key = d.pop("to_key")

        limit = d.pop("limit")

        scanned = d.pop("scanned")

        reprocessed = d.pop("reprocessed")

        skipped = d.pop("skipped")

        failed = d.pop("failed")

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

        cancel_requested = d.pop("cancel_requested")

        created_at_millis = d.pop("created_at_millis")

        last_updated_at_millis = d.pop("last_updated_at_millis")

        expires_at_millis = d.pop("expires_at_millis")

        def _parse_next_key(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        next_key = _parse_next_key(d.pop("next_key", UNSET))

        def _parse_last_error(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        last_error = _parse_last_error(d.pop("last_error", UNSET))

        document_artifact_reprocess_job = cls(
            job_id=job_id,
            attempt_id=attempt_id,
            table_name=table_name,
            artifact_name=artifact_name,
            phase=phase,
            reprocess_status=reprocess_status,
            from_key=from_key,
            to_key=to_key,
            limit=limit,
            scanned=scanned,
            reprocessed=reprocessed,
            skipped=skipped,
            failed=failed,
            pending_shards=pending_shards,
            failures=failures,
            shard_cursors=shard_cursors,
            cancel_requested=cancel_requested,
            created_at_millis=created_at_millis,
            last_updated_at_millis=last_updated_at_millis,
            expires_at_millis=expires_at_millis,
            next_key=next_key,
            last_error=last_error,
        )

        document_artifact_reprocess_job.additional_properties = d
        return document_artifact_reprocess_job

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
