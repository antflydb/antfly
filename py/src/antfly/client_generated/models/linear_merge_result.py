from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

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
        status (LinearMergePageStatus): Status of a linear merge page operation:
            - "success": All records in batch processed successfully
            - "partial": Processing stopped at shard boundary, client should retry with next_cursor
            - "error": Fatal error occurred, no records processed successfully
        upserted (int): Records inserted or updated (0 if dry_run=true)
        skipped (int): Records skipped because content hash matched (unchanged)
        deleted (int): Records deleted or would be deleted (if dry_run=true)
        next_cursor (str): ID of last record in this batch (use for next request)
        deleted_ids (Union[Unset, list[str]]): IDs that were deleted (or would be deleted if dry_run=true). Only
            included if dry_run=true.
        failed (Union[Unset, list['FailedOperation']]):
        key_range (Union[Unset, KeyRange]): Key range processed in this request
        keys_scanned (Union[Unset, int]): Total number of keys scanned from Antfly during range query
        message (Union[Unset, str]): Additional information (e.g., "stopped at shard boundary", "dry run - no changes
            made")
        took (Union[Unset, int]):
    """

    status: LinearMergePageStatus
    upserted: int
    skipped: int
    deleted: int
    next_cursor: str
    deleted_ids: Union[Unset, list[str]] = UNSET
    failed: Union[Unset, list["FailedOperation"]] = UNSET
    key_range: Union[Unset, "KeyRange"] = UNSET
    keys_scanned: Union[Unset, int] = UNSET
    message: Union[Unset, str] = UNSET
    took: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        upserted = self.upserted

        skipped = self.skipped

        deleted = self.deleted

        next_cursor = self.next_cursor

        deleted_ids: Union[Unset, list[str]] = UNSET
        if not isinstance(self.deleted_ids, Unset):
            deleted_ids = self.deleted_ids

        failed: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.failed, Unset):
            failed = []
            for failed_item_data in self.failed:
                failed_item = failed_item_data.to_dict()
                failed.append(failed_item)

        key_range: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.key_range, Unset):
            key_range = self.key_range.to_dict()

        keys_scanned = self.keys_scanned

        message = self.message

        took = self.took

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "status": status,
                "upserted": upserted,
                "skipped": skipped,
                "deleted": deleted,
                "next_cursor": next_cursor,
            }
        )
        if deleted_ids is not UNSET:
            field_dict["deleted_ids"] = deleted_ids
        if failed is not UNSET:
            field_dict["failed"] = failed
        if key_range is not UNSET:
            field_dict["key_range"] = key_range
        if keys_scanned is not UNSET:
            field_dict["keys_scanned"] = keys_scanned
        if message is not UNSET:
            field_dict["message"] = message
        if took is not UNSET:
            field_dict["took"] = took

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

        next_cursor = d.pop("next_cursor")

        deleted_ids = cast(list[str], d.pop("deleted_ids", UNSET))

        failed = []
        _failed = d.pop("failed", UNSET)
        for failed_item_data in _failed or []:
            failed_item = FailedOperation.from_dict(failed_item_data)

            failed.append(failed_item)

        _key_range = d.pop("key_range", UNSET)
        key_range: Union[Unset, KeyRange]
        if isinstance(_key_range, Unset):
            key_range = UNSET
        else:
            key_range = KeyRange.from_dict(_key_range)

        keys_scanned = d.pop("keys_scanned", UNSET)

        message = d.pop("message", UNSET)

        took = d.pop("took", UNSET)

        linear_merge_result = cls(
            status=status,
            upserted=upserted,
            skipped=skipped,
            deleted=deleted,
            next_cursor=next_cursor,
            deleted_ids=deleted_ids,
            failed=failed,
            key_range=key_range,
            keys_scanned=keys_scanned,
            message=message,
            took=took,
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
