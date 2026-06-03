from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ClusterDataRangeStatus")


@_attrs_define
class ClusterDataRangeStatus:
    """
    Attributes:
        group_id (int):
        range_id (int):
        table_id (int):
        table_name (str | Unset):
        start_key (str | Unset):
        end_key (None | str | Unset):
        doc_identity_shard_id (int | Unset):
        doc_identity_range_id (int | Unset):
        state (str | Unset):
        leader_data_id (int | None | Unset):
        voter_count (int | Unset):
        doc_count (int | Unset):
        disk_bytes (int | Unset):
        empty (bool | Unset):
    """

    group_id: int
    range_id: int
    table_id: int
    table_name: str | Unset = UNSET
    start_key: str | Unset = UNSET
    end_key: None | str | Unset = UNSET
    doc_identity_shard_id: int | Unset = UNSET
    doc_identity_range_id: int | Unset = UNSET
    state: str | Unset = UNSET
    leader_data_id: int | None | Unset = UNSET
    voter_count: int | Unset = UNSET
    doc_count: int | Unset = UNSET
    disk_bytes: int | Unset = UNSET
    empty: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        group_id = self.group_id

        range_id = self.range_id

        table_id = self.table_id

        table_name = self.table_name

        start_key = self.start_key

        end_key: None | str | Unset
        if isinstance(self.end_key, Unset):
            end_key = UNSET
        else:
            end_key = self.end_key

        doc_identity_shard_id = self.doc_identity_shard_id

        doc_identity_range_id = self.doc_identity_range_id

        state = self.state

        leader_data_id: int | None | Unset
        if isinstance(self.leader_data_id, Unset):
            leader_data_id = UNSET
        else:
            leader_data_id = self.leader_data_id

        voter_count = self.voter_count

        doc_count = self.doc_count

        disk_bytes = self.disk_bytes

        empty = self.empty

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "group_id": group_id,
                "range_id": range_id,
                "table_id": table_id,
            }
        )
        if table_name is not UNSET:
            field_dict["table_name"] = table_name
        if start_key is not UNSET:
            field_dict["start_key"] = start_key
        if end_key is not UNSET:
            field_dict["end_key"] = end_key
        if doc_identity_shard_id is not UNSET:
            field_dict["doc_identity_shard_id"] = doc_identity_shard_id
        if doc_identity_range_id is not UNSET:
            field_dict["doc_identity_range_id"] = doc_identity_range_id
        if state is not UNSET:
            field_dict["state"] = state
        if leader_data_id is not UNSET:
            field_dict["leader_data_id"] = leader_data_id
        if voter_count is not UNSET:
            field_dict["voter_count"] = voter_count
        if doc_count is not UNSET:
            field_dict["doc_count"] = doc_count
        if disk_bytes is not UNSET:
            field_dict["disk_bytes"] = disk_bytes
        if empty is not UNSET:
            field_dict["empty"] = empty

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        group_id = d.pop("group_id")

        range_id = d.pop("range_id")

        table_id = d.pop("table_id")

        table_name = d.pop("table_name", UNSET)

        start_key = d.pop("start_key", UNSET)

        def _parse_end_key(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        end_key = _parse_end_key(d.pop("end_key", UNSET))

        doc_identity_shard_id = d.pop("doc_identity_shard_id", UNSET)

        doc_identity_range_id = d.pop("doc_identity_range_id", UNSET)

        state = d.pop("state", UNSET)

        def _parse_leader_data_id(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        leader_data_id = _parse_leader_data_id(d.pop("leader_data_id", UNSET))

        voter_count = d.pop("voter_count", UNSET)

        doc_count = d.pop("doc_count", UNSET)

        disk_bytes = d.pop("disk_bytes", UNSET)

        empty = d.pop("empty", UNSET)

        cluster_data_range_status = cls(
            group_id=group_id,
            range_id=range_id,
            table_id=table_id,
            table_name=table_name,
            start_key=start_key,
            end_key=end_key,
            doc_identity_shard_id=doc_identity_shard_id,
            doc_identity_range_id=doc_identity_range_id,
            state=state,
            leader_data_id=leader_data_id,
            voter_count=voter_count,
            doc_count=doc_count,
            disk_bytes=disk_bytes,
            empty=empty,
        )

        cluster_data_range_status.additional_properties = d
        return cluster_data_range_status

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
