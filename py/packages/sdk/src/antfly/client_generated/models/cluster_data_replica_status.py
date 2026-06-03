from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ClusterDataReplicaStatus")


@_attrs_define
class ClusterDataReplicaStatus:
    """
    Attributes:
        group_id (int):
        data_id (int):
        node_id (int):
        replica_id (int):
        peer_node_ids (list[int] | Unset):
    """

    group_id: int
    data_id: int
    node_id: int
    replica_id: int
    peer_node_ids: list[int] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        group_id = self.group_id

        data_id = self.data_id

        node_id = self.node_id

        replica_id = self.replica_id

        peer_node_ids: list[int] | Unset = UNSET
        if not isinstance(self.peer_node_ids, Unset):
            peer_node_ids = self.peer_node_ids

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "group_id": group_id,
                "data_id": data_id,
                "node_id": node_id,
                "replica_id": replica_id,
            }
        )
        if peer_node_ids is not UNSET:
            field_dict["peer_node_ids"] = peer_node_ids

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        group_id = d.pop("group_id")

        data_id = d.pop("data_id")

        node_id = d.pop("node_id")

        replica_id = d.pop("replica_id")

        peer_node_ids = cast(list[int], d.pop("peer_node_ids", UNSET))

        cluster_data_replica_status = cls(
            group_id=group_id,
            data_id=data_id,
            node_id=node_id,
            replica_id=replica_id,
            peer_node_ids=peer_node_ids,
        )

        cluster_data_replica_status.additional_properties = d
        return cluster_data_replica_status

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
