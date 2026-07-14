from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.restore_job_phase import RestoreJobPhase
from ..models.restore_job_scope import RestoreJobScope
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.restore_job_result import RestoreJobResult


T = TypeVar("T", bound="RestoreJob")


@_attrs_define
class RestoreJob:
    """
    Attributes:
        job_id (str): Opaque durable restore-job identifier. Clients must not parse it as a number.
        attempt_id (int):
        scope (RestoreJobScope):
        backup_id (str):
        phase (RestoreJobPhase):
        cancel_requested (bool):
        durability_pending_table_count (int): Number of tables whose generation publication is visible but whose parent-
            directory durability could not be confirmed.
        published_table_count (int): Number of table restore intents durably published. Published tables are adopted,
            not republished, after failover.
        completed_table_count (int): Number of published tables whose placement replicas completed restore and whose
            completion checkpoint is durable.
        created_at_ms (int):
        updated_at_ms (int):
        table_name (str | Unset):
        total_table_count (int | Unset): Requested table count when known before execution.
        result (RestoreJobResult | Unset): Bounded terminal result. A committed result with durability pending means
            publication is visible but
            parent-directory durability was not confirmed. Cluster restores report aggregate triggered, committed,
            durability-pending, skipped, and failed table counts plus a bounded sample of failure details.
            `failure_details_truncated` indicates that additional failures or part of a long failure detail were omitted.
            Any failed or durability-pending table makes the job phase `failed`; inspect this result for partial progress
            and use a new idempotency key when retrying a changed request.
        error (str | Unset):
        expires_at_ms (int | Unset): Unix epoch milliseconds after which this terminal job record and its idempotency
            key may be removed. Omitted while the job is nonterminal.
    """

    job_id: str
    attempt_id: int
    scope: RestoreJobScope
    backup_id: str
    phase: RestoreJobPhase
    cancel_requested: bool
    durability_pending_table_count: int
    published_table_count: int
    completed_table_count: int
    created_at_ms: int
    updated_at_ms: int
    table_name: str | Unset = UNSET
    total_table_count: int | Unset = UNSET
    result: RestoreJobResult | Unset = UNSET
    error: str | Unset = UNSET
    expires_at_ms: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        job_id = self.job_id

        attempt_id = self.attempt_id

        scope = self.scope.value

        backup_id = self.backup_id

        phase = self.phase.value

        cancel_requested = self.cancel_requested

        durability_pending_table_count = self.durability_pending_table_count

        published_table_count = self.published_table_count

        completed_table_count = self.completed_table_count

        created_at_ms = self.created_at_ms

        updated_at_ms = self.updated_at_ms

        table_name = self.table_name

        total_table_count = self.total_table_count

        result: dict[str, Any] | Unset = UNSET
        if not isinstance(self.result, Unset):
            result = self.result.to_dict()

        error = self.error

        expires_at_ms = self.expires_at_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "job_id": job_id,
                "attempt_id": attempt_id,
                "scope": scope,
                "backup_id": backup_id,
                "phase": phase,
                "cancel_requested": cancel_requested,
                "durability_pending_table_count": durability_pending_table_count,
                "published_table_count": published_table_count,
                "completed_table_count": completed_table_count,
                "created_at_ms": created_at_ms,
                "updated_at_ms": updated_at_ms,
            }
        )
        if table_name is not UNSET:
            field_dict["table_name"] = table_name
        if total_table_count is not UNSET:
            field_dict["total_table_count"] = total_table_count
        if result is not UNSET:
            field_dict["result"] = result
        if error is not UNSET:
            field_dict["error"] = error
        if expires_at_ms is not UNSET:
            field_dict["expires_at_ms"] = expires_at_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.restore_job_result import RestoreJobResult

        d = dict(src_dict)
        job_id = d.pop("job_id")

        attempt_id = d.pop("attempt_id")

        scope = RestoreJobScope(d.pop("scope"))

        backup_id = d.pop("backup_id")

        phase = RestoreJobPhase(d.pop("phase"))

        cancel_requested = d.pop("cancel_requested")

        durability_pending_table_count = d.pop("durability_pending_table_count")

        published_table_count = d.pop("published_table_count")

        completed_table_count = d.pop("completed_table_count")

        created_at_ms = d.pop("created_at_ms")

        updated_at_ms = d.pop("updated_at_ms")

        table_name = d.pop("table_name", UNSET)

        total_table_count = d.pop("total_table_count", UNSET)

        _result = d.pop("result", UNSET)
        result: RestoreJobResult | Unset
        if isinstance(_result, Unset):
            result = UNSET
        else:
            result = RestoreJobResult.from_dict(_result)

        error = d.pop("error", UNSET)

        expires_at_ms = d.pop("expires_at_ms", UNSET)

        restore_job = cls(
            job_id=job_id,
            attempt_id=attempt_id,
            scope=scope,
            backup_id=backup_id,
            phase=phase,
            cancel_requested=cancel_requested,
            durability_pending_table_count=durability_pending_table_count,
            published_table_count=published_table_count,
            completed_table_count=completed_table_count,
            created_at_ms=created_at_ms,
            updated_at_ms=updated_at_ms,
            table_name=table_name,
            total_table_count=total_table_count,
            result=result,
            error=error,
            expires_at_ms=expires_at_ms,
        )

        restore_job.additional_properties = d
        return restore_job

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
