from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="TransactionSessionStatus")


@_attrs_define
class TransactionSessionStatus:
    """
    Attributes:
        transaction_id (str):
        owner_node_id (int):
        begin_timestamp (int):
        last_touched_timestamp (int):
        lease_expires_at (int):
        lease_state (str):
        sync_level (str):
        staged_table_count (int):
        staged_read_count (int):
        staged_write_count (int):
        staged_delete_count (int):
        read_snapshot_count (int):
        savepoint_count (int):
        durable (bool):
        savepoint_limit (int | None | Unset):
        remaining_savepoints (int | None | Unset):
    """

    transaction_id: str
    owner_node_id: int
    begin_timestamp: int
    last_touched_timestamp: int
    lease_expires_at: int
    lease_state: str
    sync_level: str
    staged_table_count: int
    staged_read_count: int
    staged_write_count: int
    staged_delete_count: int
    read_snapshot_count: int
    savepoint_count: int
    durable: bool
    savepoint_limit: int | None | Unset = UNSET
    remaining_savepoints: int | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        transaction_id = self.transaction_id

        owner_node_id = self.owner_node_id

        begin_timestamp = self.begin_timestamp

        last_touched_timestamp = self.last_touched_timestamp

        lease_expires_at = self.lease_expires_at

        lease_state = self.lease_state

        sync_level = self.sync_level

        staged_table_count = self.staged_table_count

        staged_read_count = self.staged_read_count

        staged_write_count = self.staged_write_count

        staged_delete_count = self.staged_delete_count

        read_snapshot_count = self.read_snapshot_count

        savepoint_count = self.savepoint_count

        durable = self.durable

        savepoint_limit: int | None | Unset
        if isinstance(self.savepoint_limit, Unset):
            savepoint_limit = UNSET
        else:
            savepoint_limit = self.savepoint_limit

        remaining_savepoints: int | None | Unset
        if isinstance(self.remaining_savepoints, Unset):
            remaining_savepoints = UNSET
        else:
            remaining_savepoints = self.remaining_savepoints

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "transaction_id": transaction_id,
                "owner_node_id": owner_node_id,
                "begin_timestamp": begin_timestamp,
                "last_touched_timestamp": last_touched_timestamp,
                "lease_expires_at": lease_expires_at,
                "lease_state": lease_state,
                "sync_level": sync_level,
                "staged_table_count": staged_table_count,
                "staged_read_count": staged_read_count,
                "staged_write_count": staged_write_count,
                "staged_delete_count": staged_delete_count,
                "read_snapshot_count": read_snapshot_count,
                "savepoint_count": savepoint_count,
                "durable": durable,
            }
        )
        if savepoint_limit is not UNSET:
            field_dict["savepoint_limit"] = savepoint_limit
        if remaining_savepoints is not UNSET:
            field_dict["remaining_savepoints"] = remaining_savepoints

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        transaction_id = d.pop("transaction_id")

        owner_node_id = d.pop("owner_node_id")

        begin_timestamp = d.pop("begin_timestamp")

        last_touched_timestamp = d.pop("last_touched_timestamp")

        lease_expires_at = d.pop("lease_expires_at")

        lease_state = d.pop("lease_state")

        sync_level = d.pop("sync_level")

        staged_table_count = d.pop("staged_table_count")

        staged_read_count = d.pop("staged_read_count")

        staged_write_count = d.pop("staged_write_count")

        staged_delete_count = d.pop("staged_delete_count")

        read_snapshot_count = d.pop("read_snapshot_count")

        savepoint_count = d.pop("savepoint_count")

        durable = d.pop("durable")

        def _parse_savepoint_limit(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        savepoint_limit = _parse_savepoint_limit(d.pop("savepoint_limit", UNSET))

        def _parse_remaining_savepoints(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        remaining_savepoints = _parse_remaining_savepoints(d.pop("remaining_savepoints", UNSET))

        transaction_session_status = cls(
            transaction_id=transaction_id,
            owner_node_id=owner_node_id,
            begin_timestamp=begin_timestamp,
            last_touched_timestamp=last_touched_timestamp,
            lease_expires_at=lease_expires_at,
            lease_state=lease_state,
            sync_level=sync_level,
            staged_table_count=staged_table_count,
            staged_read_count=staged_read_count,
            staged_write_count=staged_write_count,
            staged_delete_count=staged_delete_count,
            read_snapshot_count=read_snapshot_count,
            savepoint_count=savepoint_count,
            durable=durable,
            savepoint_limit=savepoint_limit,
            remaining_savepoints=remaining_savepoints,
        )

        transaction_session_status.additional_properties = d
        return transaction_session_status

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
