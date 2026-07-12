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
        job_id (int):
        attempt_id (int):
        scope (RestoreJobScope):
        backup_id (str):
        phase (RestoreJobPhase):
        cancel_requested (bool):
        created_at_ms (int):
        updated_at_ms (int):
        expires_at_ms (int): Unix epoch milliseconds after which this terminal job record and its idempotency key may be
            removed.
        table_name (str | Unset):
        completed_table_count (int | Unset): Number of table restore boundaries durably completed. Completed boundaries
            are not repeated after restart.
        total_table_count (int | Unset): Requested table count when known before execution.
        result (RestoreJobResult | Unset):
        error (str | Unset):
    """

    job_id: int
    attempt_id: int
    scope: RestoreJobScope
    backup_id: str
    phase: RestoreJobPhase
    cancel_requested: bool
    created_at_ms: int
    updated_at_ms: int
    expires_at_ms: int
    table_name: str | Unset = UNSET
    completed_table_count: int | Unset = UNSET
    total_table_count: int | Unset = UNSET
    result: RestoreJobResult | Unset = UNSET
    error: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        job_id = self.job_id

        attempt_id = self.attempt_id

        scope = self.scope.value

        backup_id = self.backup_id

        phase = self.phase.value

        cancel_requested = self.cancel_requested

        created_at_ms = self.created_at_ms

        updated_at_ms = self.updated_at_ms

        expires_at_ms = self.expires_at_ms

        table_name = self.table_name

        completed_table_count = self.completed_table_count

        total_table_count = self.total_table_count

        result: dict[str, Any] | Unset = UNSET
        if not isinstance(self.result, Unset):
            result = self.result.to_dict()

        error = self.error

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
                "created_at_ms": created_at_ms,
                "updated_at_ms": updated_at_ms,
                "expires_at_ms": expires_at_ms,
            }
        )
        if table_name is not UNSET:
            field_dict["table_name"] = table_name
        if completed_table_count is not UNSET:
            field_dict["completed_table_count"] = completed_table_count
        if total_table_count is not UNSET:
            field_dict["total_table_count"] = total_table_count
        if result is not UNSET:
            field_dict["result"] = result
        if error is not UNSET:
            field_dict["error"] = error

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

        created_at_ms = d.pop("created_at_ms")

        updated_at_ms = d.pop("updated_at_ms")

        expires_at_ms = d.pop("expires_at_ms")

        table_name = d.pop("table_name", UNSET)

        completed_table_count = d.pop("completed_table_count", UNSET)

        total_table_count = d.pop("total_table_count", UNSET)

        _result = d.pop("result", UNSET)
        result: RestoreJobResult | Unset
        if isinstance(_result, Unset):
            result = UNSET
        else:
            result = RestoreJobResult.from_dict(_result)

        error = d.pop("error", UNSET)

        restore_job = cls(
            job_id=job_id,
            attempt_id=attempt_id,
            scope=scope,
            backup_id=backup_id,
            phase=phase,
            cancel_requested=cancel_requested,
            created_at_ms=created_at_ms,
            updated_at_ms=updated_at_ms,
            expires_at_ms=expires_at_ms,
            table_name=table_name,
            completed_table_count=completed_table_count,
            total_table_count=total_table_count,
            result=result,
            error=error,
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
