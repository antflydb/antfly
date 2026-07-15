from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_index_repair_latest import RelationalIndexRepairLatest
    from ..models.relational_index_repair_report import RelationalIndexRepairReport


T = TypeVar("T", bound="RelationalIndexRepairStatus")


@_attrs_define
class RelationalIndexRepairStatus:
    """Aggregate durable repair-job evidence for schema-backed relational indexes on this table.

    Attributes:
        job_count (int | Unset):
        active_job_count (int | Unset):
        completed_job_count (int | Unset):
        failed_job_count (int | Unset):
        total_ranges_scanned (int | Unset):
        total_ranges_repaired (int | Unset):
        total_ranges_missing (int | Unset):
        aggregate_report (RelationalIndexRepairReport | Unset): Counters from relational column-backed index repair.
            These counters describe disposable derived-index artifacts rebuilt from authoritative packed rows.
        latest (RelationalIndexRepairLatest | Unset): Most recently updated durable relational index repair job for this
            table.
    """

    job_count: int | Unset = UNSET
    active_job_count: int | Unset = UNSET
    completed_job_count: int | Unset = UNSET
    failed_job_count: int | Unset = UNSET
    total_ranges_scanned: int | Unset = UNSET
    total_ranges_repaired: int | Unset = UNSET
    total_ranges_missing: int | Unset = UNSET
    aggregate_report: RelationalIndexRepairReport | Unset = UNSET
    latest: RelationalIndexRepairLatest | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        job_count = self.job_count

        active_job_count = self.active_job_count

        completed_job_count = self.completed_job_count

        failed_job_count = self.failed_job_count

        total_ranges_scanned = self.total_ranges_scanned

        total_ranges_repaired = self.total_ranges_repaired

        total_ranges_missing = self.total_ranges_missing

        aggregate_report: dict[str, Any] | Unset = UNSET
        if not isinstance(self.aggregate_report, Unset):
            aggregate_report = self.aggregate_report.to_dict()

        latest: dict[str, Any] | Unset = UNSET
        if not isinstance(self.latest, Unset):
            latest = self.latest.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if job_count is not UNSET:
            field_dict["job_count"] = job_count
        if active_job_count is not UNSET:
            field_dict["active_job_count"] = active_job_count
        if completed_job_count is not UNSET:
            field_dict["completed_job_count"] = completed_job_count
        if failed_job_count is not UNSET:
            field_dict["failed_job_count"] = failed_job_count
        if total_ranges_scanned is not UNSET:
            field_dict["total_ranges_scanned"] = total_ranges_scanned
        if total_ranges_repaired is not UNSET:
            field_dict["total_ranges_repaired"] = total_ranges_repaired
        if total_ranges_missing is not UNSET:
            field_dict["total_ranges_missing"] = total_ranges_missing
        if aggregate_report is not UNSET:
            field_dict["aggregate_report"] = aggregate_report
        if latest is not UNSET:
            field_dict["latest"] = latest

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_index_repair_latest import RelationalIndexRepairLatest
        from ..models.relational_index_repair_report import RelationalIndexRepairReport

        d = dict(src_dict)
        job_count = d.pop("job_count", UNSET)

        active_job_count = d.pop("active_job_count", UNSET)

        completed_job_count = d.pop("completed_job_count", UNSET)

        failed_job_count = d.pop("failed_job_count", UNSET)

        total_ranges_scanned = d.pop("total_ranges_scanned", UNSET)

        total_ranges_repaired = d.pop("total_ranges_repaired", UNSET)

        total_ranges_missing = d.pop("total_ranges_missing", UNSET)

        _aggregate_report = d.pop("aggregate_report", UNSET)
        aggregate_report: RelationalIndexRepairReport | Unset
        if isinstance(_aggregate_report, Unset):
            aggregate_report = UNSET
        else:
            aggregate_report = RelationalIndexRepairReport.from_dict(_aggregate_report)

        _latest = d.pop("latest", UNSET)
        latest: RelationalIndexRepairLatest | Unset
        if isinstance(_latest, Unset):
            latest = UNSET
        else:
            latest = RelationalIndexRepairLatest.from_dict(_latest)

        relational_index_repair_status = cls(
            job_count=job_count,
            active_job_count=active_job_count,
            completed_job_count=completed_job_count,
            failed_job_count=failed_job_count,
            total_ranges_scanned=total_ranges_scanned,
            total_ranges_repaired=total_ranges_repaired,
            total_ranges_missing=total_ranges_missing,
            aggregate_report=aggregate_report,
            latest=latest,
        )

        relational_index_repair_status.additional_properties = d
        return relational_index_repair_status

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
