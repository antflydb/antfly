from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.restore_job import RestoreJob


T = TypeVar("T", bound="RestoreJobList")


@_attrs_define
class RestoreJobList:
    """
    Attributes:
        jobs (list[RestoreJob]):
        next_cursor (str | Unset): Opaque newest-first continuation cursor. Omitted when no additional authorized jobs
            match the filters.
    """

    jobs: list[RestoreJob]
    next_cursor: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        jobs = []
        for jobs_item_data in self.jobs:
            jobs_item = jobs_item_data.to_dict()
            jobs.append(jobs_item)

        next_cursor = self.next_cursor

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "jobs": jobs,
            }
        )
        if next_cursor is not UNSET:
            field_dict["next_cursor"] = next_cursor

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.restore_job import RestoreJob

        d = dict(src_dict)
        jobs = []
        _jobs = d.pop("jobs")
        for jobs_item_data in _jobs:
            jobs_item = RestoreJob.from_dict(jobs_item_data)

            jobs.append(jobs_item)

        next_cursor = d.pop("next_cursor", UNSET)

        restore_job_list = cls(
            jobs=jobs,
            next_cursor=next_cursor,
        )

        return restore_job_list
