from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_column_backed_index_repair_report import RelationalColumnBackedIndexRepairReport


T = TypeVar("T", bound="RelationalIndexRepairJobRecord")


@_attrs_define
class RelationalIndexRepairJobRecord:
    """
    Attributes:
        version (int):
        job_id (str):
        database_name (str):
        namespace_name (str):
        table_name (str):
        worker_id (str):
        lower_doc_key (str):
        upper_doc_key (str):
        lease_ms (int):
        max_work_units (int):
        status (str):
        created_at_ns (int):
        updated_at_ns (int):
        attempts (int):
        completed (bool):
        next_lower_doc_key (str):
        last_ranges_scanned (int):
        last_ranges_repaired (int):
        last_ranges_missing (int):
        total_ranges_scanned (int):
        total_ranges_repaired (int):
        total_ranges_missing (int):
        last_report (RelationalColumnBackedIndexRepairReport):
        aggregate_report (RelationalColumnBackedIndexRepairReport):
        complete (bool | None | Unset):
        last_error (None | str | Unset):
    """

    version: int
    job_id: str
    database_name: str
    namespace_name: str
    table_name: str
    worker_id: str
    lower_doc_key: str
    upper_doc_key: str
    lease_ms: int
    max_work_units: int
    status: str
    created_at_ns: int
    updated_at_ns: int
    attempts: int
    completed: bool
    next_lower_doc_key: str
    last_ranges_scanned: int
    last_ranges_repaired: int
    last_ranges_missing: int
    total_ranges_scanned: int
    total_ranges_repaired: int
    total_ranges_missing: int
    last_report: RelationalColumnBackedIndexRepairReport
    aggregate_report: RelationalColumnBackedIndexRepairReport
    complete: bool | None | Unset = UNSET
    last_error: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        version = self.version

        job_id = self.job_id

        database_name = self.database_name

        namespace_name = self.namespace_name

        table_name = self.table_name

        worker_id = self.worker_id

        lower_doc_key = self.lower_doc_key

        upper_doc_key = self.upper_doc_key

        lease_ms = self.lease_ms

        max_work_units = self.max_work_units

        status = self.status

        created_at_ns = self.created_at_ns

        updated_at_ns = self.updated_at_ns

        attempts = self.attempts

        completed = self.completed

        next_lower_doc_key = self.next_lower_doc_key

        last_ranges_scanned = self.last_ranges_scanned

        last_ranges_repaired = self.last_ranges_repaired

        last_ranges_missing = self.last_ranges_missing

        total_ranges_scanned = self.total_ranges_scanned

        total_ranges_repaired = self.total_ranges_repaired

        total_ranges_missing = self.total_ranges_missing

        last_report = self.last_report.to_dict()

        aggregate_report = self.aggregate_report.to_dict()

        complete: bool | None | Unset
        if isinstance(self.complete, Unset):
            complete = UNSET
        else:
            complete = self.complete

        last_error: None | str | Unset
        if isinstance(self.last_error, Unset):
            last_error = UNSET
        else:
            last_error = self.last_error

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "version": version,
                "job_id": job_id,
                "database_name": database_name,
                "namespace_name": namespace_name,
                "table_name": table_name,
                "worker_id": worker_id,
                "lower_doc_key": lower_doc_key,
                "upper_doc_key": upper_doc_key,
                "lease_ms": lease_ms,
                "max_work_units": max_work_units,
                "status": status,
                "created_at_ns": created_at_ns,
                "updated_at_ns": updated_at_ns,
                "attempts": attempts,
                "completed": completed,
                "next_lower_doc_key": next_lower_doc_key,
                "last_ranges_scanned": last_ranges_scanned,
                "last_ranges_repaired": last_ranges_repaired,
                "last_ranges_missing": last_ranges_missing,
                "total_ranges_scanned": total_ranges_scanned,
                "total_ranges_repaired": total_ranges_repaired,
                "total_ranges_missing": total_ranges_missing,
                "last_report": last_report,
                "aggregate_report": aggregate_report,
            }
        )
        if complete is not UNSET:
            field_dict["complete"] = complete
        if last_error is not UNSET:
            field_dict["last_error"] = last_error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_column_backed_index_repair_report import RelationalColumnBackedIndexRepairReport

        d = dict(src_dict)
        version = d.pop("version")

        job_id = d.pop("job_id")

        database_name = d.pop("database_name")

        namespace_name = d.pop("namespace_name")

        table_name = d.pop("table_name")

        worker_id = d.pop("worker_id")

        lower_doc_key = d.pop("lower_doc_key")

        upper_doc_key = d.pop("upper_doc_key")

        lease_ms = d.pop("lease_ms")

        max_work_units = d.pop("max_work_units")

        status = d.pop("status")

        created_at_ns = d.pop("created_at_ns")

        updated_at_ns = d.pop("updated_at_ns")

        attempts = d.pop("attempts")

        completed = d.pop("completed")

        next_lower_doc_key = d.pop("next_lower_doc_key")

        last_ranges_scanned = d.pop("last_ranges_scanned")

        last_ranges_repaired = d.pop("last_ranges_repaired")

        last_ranges_missing = d.pop("last_ranges_missing")

        total_ranges_scanned = d.pop("total_ranges_scanned")

        total_ranges_repaired = d.pop("total_ranges_repaired")

        total_ranges_missing = d.pop("total_ranges_missing")

        last_report = RelationalColumnBackedIndexRepairReport.from_dict(d.pop("last_report"))

        aggregate_report = RelationalColumnBackedIndexRepairReport.from_dict(d.pop("aggregate_report"))

        def _parse_complete(data: object) -> bool | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(bool | None | Unset, data)

        complete = _parse_complete(d.pop("complete", UNSET))

        def _parse_last_error(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        last_error = _parse_last_error(d.pop("last_error", UNSET))

        relational_index_repair_job_record = cls(
            version=version,
            job_id=job_id,
            database_name=database_name,
            namespace_name=namespace_name,
            table_name=table_name,
            worker_id=worker_id,
            lower_doc_key=lower_doc_key,
            upper_doc_key=upper_doc_key,
            lease_ms=lease_ms,
            max_work_units=max_work_units,
            status=status,
            created_at_ns=created_at_ns,
            updated_at_ns=updated_at_ns,
            attempts=attempts,
            completed=completed,
            next_lower_doc_key=next_lower_doc_key,
            last_ranges_scanned=last_ranges_scanned,
            last_ranges_repaired=last_ranges_repaired,
            last_ranges_missing=last_ranges_missing,
            total_ranges_scanned=total_ranges_scanned,
            total_ranges_repaired=total_ranges_repaired,
            total_ranges_missing=total_ranges_missing,
            last_report=last_report,
            aggregate_report=aggregate_report,
            complete=complete,
            last_error=last_error,
        )

        return relational_index_repair_job_record
