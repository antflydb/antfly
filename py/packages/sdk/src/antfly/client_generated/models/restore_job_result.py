from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.restore_job_result_restore import RestoreJobResultRestore
from ..models.restore_job_result_status import RestoreJobResultStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.restore_job_result_failure_details_item import RestoreJobResultFailureDetailsItem


T = TypeVar("T", bound="RestoreJobResult")


@_attrs_define
class RestoreJobResult:
    """Bounded terminal result. Cluster restores report aggregate triggered, skipped, and failed table counts plus
    a bounded sample of failure details. `failure_details_truncated` indicates that additional failures or part
    of a long failure detail were omitted. Any failed table makes the job phase `failed`; inspect this result for
    partial
    progress and use a new idempotency key when retrying a changed request.

        Attributes:
            restore (RestoreJobResultRestore | Unset): Present for a successful single-table restore.
            status (RestoreJobResultStatus | Unset): Aggregate terminal status for a cluster restore.
            triggered_table_count (int | Unset):
            committed_table_count (int | Unset):
            skipped_table_count (int | Unset):
            failed_table_count (int | Unset):
            failure_details (list[RestoreJobResultFailureDetailsItem] | Unset):
            failure_details_truncated (bool | Unset): True when additional failed tables or part of a long table name or
                error were omitted.
    """

    restore: RestoreJobResultRestore | Unset = UNSET
    status: RestoreJobResultStatus | Unset = UNSET
    triggered_table_count: int | Unset = UNSET
    committed_table_count: int | Unset = UNSET
    skipped_table_count: int | Unset = UNSET
    failed_table_count: int | Unset = UNSET
    failure_details: list[RestoreJobResultFailureDetailsItem] | Unset = UNSET
    failure_details_truncated: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        restore: str | Unset = UNSET
        if not isinstance(self.restore, Unset):
            restore = self.restore.value

        status: str | Unset = UNSET
        if not isinstance(self.status, Unset):
            status = self.status.value

        triggered_table_count = self.triggered_table_count

        committed_table_count = self.committed_table_count

        skipped_table_count = self.skipped_table_count

        failed_table_count = self.failed_table_count

        failure_details: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.failure_details, Unset):
            failure_details = []
            for failure_details_item_data in self.failure_details:
                failure_details_item = failure_details_item_data.to_dict()
                failure_details.append(failure_details_item)

        failure_details_truncated = self.failure_details_truncated

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if restore is not UNSET:
            field_dict["restore"] = restore
        if status is not UNSET:
            field_dict["status"] = status
        if triggered_table_count is not UNSET:
            field_dict["triggered_table_count"] = triggered_table_count
        if committed_table_count is not UNSET:
            field_dict["committed_table_count"] = committed_table_count
        if skipped_table_count is not UNSET:
            field_dict["skipped_table_count"] = skipped_table_count
        if failed_table_count is not UNSET:
            field_dict["failed_table_count"] = failed_table_count
        if failure_details is not UNSET:
            field_dict["failure_details"] = failure_details
        if failure_details_truncated is not UNSET:
            field_dict["failure_details_truncated"] = failure_details_truncated

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.restore_job_result_failure_details_item import RestoreJobResultFailureDetailsItem

        d = dict(src_dict)
        _restore = d.pop("restore", UNSET)
        restore: RestoreJobResultRestore | Unset
        if isinstance(_restore, Unset):
            restore = UNSET
        else:
            restore = RestoreJobResultRestore(_restore)

        _status = d.pop("status", UNSET)
        status: RestoreJobResultStatus | Unset
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = RestoreJobResultStatus(_status)

        triggered_table_count = d.pop("triggered_table_count", UNSET)

        committed_table_count = d.pop("committed_table_count", UNSET)

        skipped_table_count = d.pop("skipped_table_count", UNSET)

        failed_table_count = d.pop("failed_table_count", UNSET)

        _failure_details = d.pop("failure_details", UNSET)
        failure_details: list[RestoreJobResultFailureDetailsItem] | Unset = UNSET
        if _failure_details is not UNSET:
            failure_details = []
            for failure_details_item_data in _failure_details:
                failure_details_item = RestoreJobResultFailureDetailsItem.from_dict(failure_details_item_data)

                failure_details.append(failure_details_item)

        failure_details_truncated = d.pop("failure_details_truncated", UNSET)

        restore_job_result = cls(
            restore=restore,
            status=status,
            triggered_table_count=triggered_table_count,
            committed_table_count=committed_table_count,
            skipped_table_count=skipped_table_count,
            failed_table_count=failed_table_count,
            failure_details=failure_details,
            failure_details_truncated=failure_details_truncated,
        )

        return restore_job_result
