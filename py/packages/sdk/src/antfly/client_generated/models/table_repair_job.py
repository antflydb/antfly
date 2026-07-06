from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.artifact_repair_kind import ArtifactRepairKind
from ..models.repair_target import RepairTarget
from ..models.table_repair_job_phase import TableRepairJobPhase
from ..models.table_repair_job_repair_status import TableRepairJobRepairStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.table_repair_run_result import TableRepairRunResult


T = TypeVar("T", bound="TableRepairJob")


@_attrs_define
class TableRepairJob:
    """Durable table repair job state.

    Attributes:
        job_id (int): Server-assigned durable repair job identifier.
        attempt_id (int): Monotonic execution attempt token for the current running pass.
        table_name (str): Table being repaired.
        phase (TableRepairJobPhase): Lifecycle phase of the repair job.
        repair_status (TableRepairJobRepairStatus): User-facing repair progress state. `debt_remaining` means the
            bounded job stopped because unsupported or failed debt still requires operator action.
        target (RepairTarget): Repair subsystem to inspect or run.
        limit (int): Effective per-pass repair limit.
        force (bool): Whether the job forces a named index rebuild.
        result (TableRepairRunResult): Result of one bounded table repair pass.
        cancel_requested (bool): Whether cancellation has been requested for a running pass. Running passes finish at a
            bounded repair boundary before the job transitions to cancelled.
        created_at_millis (int): Unix epoch milliseconds when the job was created.
        last_updated_at_millis (int): Unix epoch milliseconds when the job state was last updated.
        expires_at_millis (int): Unix epoch milliseconds when the job is eligible for cleanup.
        kind (ArtifactRepairKind | Unset): Kind of stored artifact tracked by the repair queue.
        index (str | Unset): Index name when the job is restricted to one index.
        cursor (None | str | Unset): Opaque continuation cursor for the next bounded repair pass.
        last_error (None | str | Unset): Last stable job-level error code.
    """

    job_id: int
    attempt_id: int
    table_name: str
    phase: TableRepairJobPhase
    repair_status: TableRepairJobRepairStatus
    target: RepairTarget
    limit: int
    force: bool
    result: TableRepairRunResult
    cancel_requested: bool
    created_at_millis: int
    last_updated_at_millis: int
    expires_at_millis: int
    kind: ArtifactRepairKind | Unset = UNSET
    index: str | Unset = UNSET
    cursor: None | str | Unset = UNSET
    last_error: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        job_id = self.job_id

        attempt_id = self.attempt_id

        table_name = self.table_name

        phase = self.phase.value

        repair_status = self.repair_status.value

        target = self.target.value

        limit = self.limit

        force = self.force

        result = self.result.to_dict()

        cancel_requested = self.cancel_requested

        created_at_millis = self.created_at_millis

        last_updated_at_millis = self.last_updated_at_millis

        expires_at_millis = self.expires_at_millis

        kind: str | Unset = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

        index = self.index

        cursor: None | str | Unset
        if isinstance(self.cursor, Unset):
            cursor = UNSET
        else:
            cursor = self.cursor

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
                "phase": phase,
                "repair_status": repair_status,
                "target": target,
                "limit": limit,
                "force": force,
                "result": result,
                "cancel_requested": cancel_requested,
                "created_at_millis": created_at_millis,
                "last_updated_at_millis": last_updated_at_millis,
                "expires_at_millis": expires_at_millis,
            }
        )
        if kind is not UNSET:
            field_dict["kind"] = kind
        if index is not UNSET:
            field_dict["index"] = index
        if cursor is not UNSET:
            field_dict["cursor"] = cursor
        if last_error is not UNSET:
            field_dict["last_error"] = last_error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.table_repair_run_result import TableRepairRunResult

        d = dict(src_dict)
        job_id = d.pop("job_id")

        attempt_id = d.pop("attempt_id")

        table_name = d.pop("table_name")

        phase = TableRepairJobPhase(d.pop("phase"))

        repair_status = TableRepairJobRepairStatus(d.pop("repair_status"))

        target = RepairTarget(d.pop("target"))

        limit = d.pop("limit")

        force = d.pop("force")

        result = TableRepairRunResult.from_dict(d.pop("result"))

        cancel_requested = d.pop("cancel_requested")

        created_at_millis = d.pop("created_at_millis")

        last_updated_at_millis = d.pop("last_updated_at_millis")

        expires_at_millis = d.pop("expires_at_millis")

        _kind = d.pop("kind", UNSET)
        kind: ArtifactRepairKind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = ArtifactRepairKind(_kind)

        index = d.pop("index", UNSET)

        def _parse_cursor(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        cursor = _parse_cursor(d.pop("cursor", UNSET))

        def _parse_last_error(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        last_error = _parse_last_error(d.pop("last_error", UNSET))

        table_repair_job = cls(
            job_id=job_id,
            attempt_id=attempt_id,
            table_name=table_name,
            phase=phase,
            repair_status=repair_status,
            target=target,
            limit=limit,
            force=force,
            result=result,
            cancel_requested=cancel_requested,
            created_at_millis=created_at_millis,
            last_updated_at_millis=last_updated_at_millis,
            expires_at_millis=expires_at_millis,
            kind=kind,
            index=index,
            cursor=cursor,
            last_error=last_error,
        )

        table_repair_job.additional_properties = d
        return table_repair_job

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
