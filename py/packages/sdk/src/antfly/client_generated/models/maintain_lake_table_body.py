from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.maintain_lake_table_body_action import MaintainLakeTableBodyAction
from ..types import UNSET, Unset

T = TypeVar("T", bound="MaintainLakeTableBody")


@_attrs_define
class MaintainLakeTableBody:
    """
    Attributes:
        action (MaintainLakeTableBodyAction):
        operation_id (str | Unset): Required for compact/vacuum/wal_gc; omitted for scheduler or enrichment status.
        dry_run (bool | Unset):  Default: True.
        exclusive_ownership (bool | Unset):  Default: False.
        max_rows (int | Unset):  Default: 16384.
        max_bytes (int | Unset):  Default: 33554432.
        retain_ms (int | Unset):  Default: 604800000.
        keep_latest (int | Unset):  Default: 2.
        max_deleted (int | Unset):  Default: 4096.
    """

    action: MaintainLakeTableBodyAction
    operation_id: str | Unset = UNSET
    dry_run: bool | Unset = True
    exclusive_ownership: bool | Unset = False
    max_rows: int | Unset = 16384
    max_bytes: int | Unset = 33554432
    retain_ms: int | Unset = 604800000
    keep_latest: int | Unset = 2
    max_deleted: int | Unset = 4096
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        action = self.action.value

        operation_id = self.operation_id

        dry_run = self.dry_run

        exclusive_ownership = self.exclusive_ownership

        max_rows = self.max_rows

        max_bytes = self.max_bytes

        retain_ms = self.retain_ms

        keep_latest = self.keep_latest

        max_deleted = self.max_deleted

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "action": action,
            }
        )
        if operation_id is not UNSET:
            field_dict["operation_id"] = operation_id
        if dry_run is not UNSET:
            field_dict["dry_run"] = dry_run
        if exclusive_ownership is not UNSET:
            field_dict["exclusive_ownership"] = exclusive_ownership
        if max_rows is not UNSET:
            field_dict["max_rows"] = max_rows
        if max_bytes is not UNSET:
            field_dict["max_bytes"] = max_bytes
        if retain_ms is not UNSET:
            field_dict["retain_ms"] = retain_ms
        if keep_latest is not UNSET:
            field_dict["keep_latest"] = keep_latest
        if max_deleted is not UNSET:
            field_dict["max_deleted"] = max_deleted

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        action = MaintainLakeTableBodyAction(d.pop("action"))

        operation_id = d.pop("operation_id", UNSET)

        dry_run = d.pop("dry_run", UNSET)

        exclusive_ownership = d.pop("exclusive_ownership", UNSET)

        max_rows = d.pop("max_rows", UNSET)

        max_bytes = d.pop("max_bytes", UNSET)

        retain_ms = d.pop("retain_ms", UNSET)

        keep_latest = d.pop("keep_latest", UNSET)

        max_deleted = d.pop("max_deleted", UNSET)

        maintain_lake_table_body = cls(
            action=action,
            operation_id=operation_id,
            dry_run=dry_run,
            exclusive_ownership=exclusive_ownership,
            max_rows=max_rows,
            max_bytes=max_bytes,
            retain_ms=retain_ms,
            keep_latest=keep_latest,
            max_deleted=max_deleted,
        )

        maintain_lake_table_body.additional_properties = d
        return maintain_lake_table_body

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
