from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_index_repair_report import RelationalIndexRepairReport


T = TypeVar("T", bound="RelationalIndexRepairLatest")


@_attrs_define
class RelationalIndexRepairLatest:
    """Most recently updated durable relational index repair job for this table.

    Attributes:
        job_id (str | Unset):
        status (str | Unset):
        worker_id (str | Unset):
        updated_at_ns (int | Unset):
        next_lower_doc_key (str | Unset):
        last_error (None | str | Unset):
        last_report (RelationalIndexRepairReport | Unset): Counters from relational column-backed index repair. These
            counters describe disposable derived-index artifacts rebuilt from authoritative packed rows.
    """

    job_id: str | Unset = UNSET
    status: str | Unset = UNSET
    worker_id: str | Unset = UNSET
    updated_at_ns: int | Unset = UNSET
    next_lower_doc_key: str | Unset = UNSET
    last_error: None | str | Unset = UNSET
    last_report: RelationalIndexRepairReport | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        job_id = self.job_id

        status = self.status

        worker_id = self.worker_id

        updated_at_ns = self.updated_at_ns

        next_lower_doc_key = self.next_lower_doc_key

        last_error: None | str | Unset
        if isinstance(self.last_error, Unset):
            last_error = UNSET
        else:
            last_error = self.last_error

        last_report: dict[str, Any] | Unset = UNSET
        if not isinstance(self.last_report, Unset):
            last_report = self.last_report.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if job_id is not UNSET:
            field_dict["job_id"] = job_id
        if status is not UNSET:
            field_dict["status"] = status
        if worker_id is not UNSET:
            field_dict["worker_id"] = worker_id
        if updated_at_ns is not UNSET:
            field_dict["updated_at_ns"] = updated_at_ns
        if next_lower_doc_key is not UNSET:
            field_dict["next_lower_doc_key"] = next_lower_doc_key
        if last_error is not UNSET:
            field_dict["last_error"] = last_error
        if last_report is not UNSET:
            field_dict["last_report"] = last_report

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_index_repair_report import RelationalIndexRepairReport

        d = dict(src_dict)
        job_id = d.pop("job_id", UNSET)

        status = d.pop("status", UNSET)

        worker_id = d.pop("worker_id", UNSET)

        updated_at_ns = d.pop("updated_at_ns", UNSET)

        next_lower_doc_key = d.pop("next_lower_doc_key", UNSET)

        def _parse_last_error(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        last_error = _parse_last_error(d.pop("last_error", UNSET))

        _last_report = d.pop("last_report", UNSET)
        last_report: RelationalIndexRepairReport | Unset
        if isinstance(_last_report, Unset):
            last_report = UNSET
        else:
            last_report = RelationalIndexRepairReport.from_dict(_last_report)

        relational_index_repair_latest = cls(
            job_id=job_id,
            status=status,
            worker_id=worker_id,
            updated_at_ns=updated_at_ns,
            next_lower_doc_key=next_lower_doc_key,
            last_error=last_error,
            last_report=last_report,
        )

        relational_index_repair_latest.additional_properties = d
        return relational_index_repair_latest

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
