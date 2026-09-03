from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.linear_merge_page_status import LinearMergePageStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.failed_operation import FailedOperation
    from ..models.key_range import KeyRange


T = TypeVar("T", bound="LinearMergeResult")


@_attrs_define
class LinearMergeResult:
    """
    Attributes:
        status (LinearMergePageStatus): Status of a completed linear merge page. Successful responses are atomic
            and use "success"; failures are returned as non-2xx HTTP responses.
        upserted (int): Records inserted or updated (0 if dry_run=true)
        skipped (int): Records skipped because content hash matched (unchanged)
        deleted (int): Records deleted or would be deleted (if dry_run=true)
        failed (list[FailedOperation]):
        next_cursor (str): ID of last record in this batch (use for next request)
        took (int):
        deleted_ids (list[str] | Unset): IDs that were deleted (or would be deleted if dry_run=true). Only included if
            dry_run=true.
        key_range (KeyRange | Unset): Key range processed in this request
        keys_scanned (int | Unset): Total number of keys scanned from Antfly during range query
        message (str | Unset): Additional information (e.g., "stopped at shard boundary", "dry run - no changes made")
    """

    status: LinearMergePageStatus
    upserted: int
    skipped: int
    deleted: int
    failed: list[FailedOperation]
    next_cursor: str
    took: int
    deleted_ids: list[str] | Unset = UNSET
    key_range: KeyRange | Unset = UNSET
    keys_scanned: int | Unset = UNSET
    message: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        upserted = self.upserted

        skipped = self.skipped

        deleted = self.deleted

        failed = []
        for failed_item_data in self.failed:
            failed_item = failed_item_data.to_dict()
            failed.append(failed_item)

        next_cursor = self.next_cursor

        took = self.took

        deleted_ids: list[str] | Unset = UNSET
        if not isinstance(self.deleted_ids, Unset):
            deleted_ids = self.deleted_ids

        key_range: dict[str, Any] | Unset = UNSET
        if not isinstance(self.key_range, Unset):
            key_range = self.key_range.to_dict()

        keys_scanned = self.keys_scanned

        message = self.message

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "status": status,
                "upserted": upserted,
                "skipped": skipped,
                "deleted": deleted,
                "failed": failed,
                "next_cursor": next_cursor,
                "took": took,
            }
        )
        if deleted_ids is not UNSET:
            field_dict["deleted_ids"] = deleted_ids
        if key_range is not UNSET:
            field_dict["key_range"] = key_range
        if keys_scanned is not UNSET:
            field_dict["keys_scanned"] = keys_scanned
        if message is not UNSET:
            field_dict["message"] = message

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.failed_operation import FailedOperation
        from ..models.key_range import KeyRange

        d = dict(src_dict)
        status = LinearMergePageStatus(d.pop("status"))

        upserted = d.pop("upserted")

        skipped = d.pop("skipped")

        deleted = d.pop("deleted")

        failed = []
        _failed = d.pop("failed")
        for failed_item_data in _failed:
            failed_item = FailedOperation.from_dict(failed_item_data)

            failed.append(failed_item)

        next_cursor = d.pop("next_cursor")

        took = d.pop("took")

        deleted_ids = cast(list[str], d.pop("deleted_ids", UNSET))

        _key_range = d.pop("key_range", UNSET)
        key_range: KeyRange | Unset
        if isinstance(_key_range, Unset):
            key_range = UNSET
        else:
            key_range = KeyRange.from_dict(_key_range)

        keys_scanned = d.pop("keys_scanned", UNSET)

        message = d.pop("message", UNSET)

        linear_merge_result = cls(
            status=status,
            upserted=upserted,
            skipped=skipped,
            deleted=deleted,
            failed=failed,
            next_cursor=next_cursor,
            took=took,
            deleted_ids=deleted_ids,
            key_range=key_range,
            keys_scanned=keys_scanned,
            message=message,
        )

        linear_merge_result.additional_properties = d
        return linear_merge_result

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
