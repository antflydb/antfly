from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="LsmStorageStatus")


@_attrs_define
class LsmStorageStatus:
    """Compact LSM backend operational status. Detailed low-level counters are available through metrics.

    Attributes:
        run_count (int | Unset):
        run_bytes (int | Unset):
        l0_run_count (int | Unset):
        l0_bytes (int | Unset):
        wal_retained_bytes (int | Unset):
        compaction_backlog_bytes (int | Unset):
        active_readers (int | Unset):
    """

    run_count: int | Unset = UNSET
    run_bytes: int | Unset = UNSET
    l0_run_count: int | Unset = UNSET
    l0_bytes: int | Unset = UNSET
    wal_retained_bytes: int | Unset = UNSET
    compaction_backlog_bytes: int | Unset = UNSET
    active_readers: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        run_count = self.run_count

        run_bytes = self.run_bytes

        l0_run_count = self.l0_run_count

        l0_bytes = self.l0_bytes

        wal_retained_bytes = self.wal_retained_bytes

        compaction_backlog_bytes = self.compaction_backlog_bytes

        active_readers = self.active_readers

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if run_count is not UNSET:
            field_dict["run_count"] = run_count
        if run_bytes is not UNSET:
            field_dict["run_bytes"] = run_bytes
        if l0_run_count is not UNSET:
            field_dict["l0_run_count"] = l0_run_count
        if l0_bytes is not UNSET:
            field_dict["l0_bytes"] = l0_bytes
        if wal_retained_bytes is not UNSET:
            field_dict["wal_retained_bytes"] = wal_retained_bytes
        if compaction_backlog_bytes is not UNSET:
            field_dict["compaction_backlog_bytes"] = compaction_backlog_bytes
        if active_readers is not UNSET:
            field_dict["active_readers"] = active_readers

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        run_count = d.pop("run_count", UNSET)

        run_bytes = d.pop("run_bytes", UNSET)

        l0_run_count = d.pop("l0_run_count", UNSET)

        l0_bytes = d.pop("l0_bytes", UNSET)

        wal_retained_bytes = d.pop("wal_retained_bytes", UNSET)

        compaction_backlog_bytes = d.pop("compaction_backlog_bytes", UNSET)

        active_readers = d.pop("active_readers", UNSET)

        lsm_storage_status = cls(
            run_count=run_count,
            run_bytes=run_bytes,
            l0_run_count=l0_run_count,
            l0_bytes=l0_bytes,
            wal_retained_bytes=wal_retained_bytes,
            compaction_backlog_bytes=compaction_backlog_bytes,
            active_readers=active_readers,
        )

        lsm_storage_status.additional_properties = d
        return lsm_storage_status

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
