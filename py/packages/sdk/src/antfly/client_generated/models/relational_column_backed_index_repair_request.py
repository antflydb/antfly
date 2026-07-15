from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalColumnBackedIndexRepairRequest")


@_attrs_define
class RelationalColumnBackedIndexRepairRequest:
    """
    Attributes:
        worker_id (str): Stable worker identifier used to claim bounded repair ranges.
        job_id (str | Unset): Optional durable repair job identifier used to persist pass progress and resume metadata.
        lease_ms (int | Unset): Lease duration for claimed repair ranges in milliseconds. Default: 60000.
        max_work_units (int | Unset): Maximum number of bounded repair ranges to claim and process in one pass. Default:
            1.
    """

    worker_id: str
    job_id: str | Unset = UNSET
    lease_ms: int | Unset = 60000
    max_work_units: int | Unset = 1

    def to_dict(self) -> dict[str, Any]:
        worker_id = self.worker_id

        job_id = self.job_id

        lease_ms = self.lease_ms

        max_work_units = self.max_work_units

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "worker_id": worker_id,
            }
        )
        if job_id is not UNSET:
            field_dict["job_id"] = job_id
        if lease_ms is not UNSET:
            field_dict["lease_ms"] = lease_ms
        if max_work_units is not UNSET:
            field_dict["max_work_units"] = max_work_units

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        worker_id = d.pop("worker_id")

        job_id = d.pop("job_id", UNSET)

        lease_ms = d.pop("lease_ms", UNSET)

        max_work_units = d.pop("max_work_units", UNSET)

        relational_column_backed_index_repair_request = cls(
            worker_id=worker_id,
            job_id=job_id,
            lease_ms=lease_ms,
            max_work_units=max_work_units,
        )

        return relational_column_backed_index_repair_request
