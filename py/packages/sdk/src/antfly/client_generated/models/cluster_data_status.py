from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.cluster_data_group_status import ClusterDataGroupStatus
    from ..models.cluster_data_node_status import ClusterDataNodeStatus
    from ..models.cluster_data_range_status import ClusterDataRangeStatus
    from ..models.cluster_data_replica_status import ClusterDataReplicaStatus


T = TypeVar("T", bound="ClusterDataStatus")


@_attrs_define
class ClusterDataStatus:
    """Typed Zig status view for table data topology and range placement.

    Attributes:
        nodes (list[ClusterDataNodeStatus] | Unset):
        ranges (list[ClusterDataRangeStatus] | Unset):
        replicas (list[ClusterDataReplicaStatus] | Unset):
        groups (list[ClusterDataGroupStatus] | Unset):
    """

    nodes: list[ClusterDataNodeStatus] | Unset = UNSET
    ranges: list[ClusterDataRangeStatus] | Unset = UNSET
    replicas: list[ClusterDataReplicaStatus] | Unset = UNSET
    groups: list[ClusterDataGroupStatus] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        nodes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.nodes, Unset):
            nodes = []
            for nodes_item_data in self.nodes:
                nodes_item = nodes_item_data.to_dict()
                nodes.append(nodes_item)

        ranges: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.ranges, Unset):
            ranges = []
            for ranges_item_data in self.ranges:
                ranges_item = ranges_item_data.to_dict()
                ranges.append(ranges_item)

        replicas: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.replicas, Unset):
            replicas = []
            for replicas_item_data in self.replicas:
                replicas_item = replicas_item_data.to_dict()
                replicas.append(replicas_item)

        groups: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.groups, Unset):
            groups = []
            for groups_item_data in self.groups:
                groups_item = groups_item_data.to_dict()
                groups.append(groups_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if nodes is not UNSET:
            field_dict["nodes"] = nodes
        if ranges is not UNSET:
            field_dict["ranges"] = ranges
        if replicas is not UNSET:
            field_dict["replicas"] = replicas
        if groups is not UNSET:
            field_dict["groups"] = groups

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.cluster_data_group_status import ClusterDataGroupStatus
        from ..models.cluster_data_node_status import ClusterDataNodeStatus
        from ..models.cluster_data_range_status import ClusterDataRangeStatus
        from ..models.cluster_data_replica_status import ClusterDataReplicaStatus

        d = dict(src_dict)
        _nodes = d.pop("nodes", UNSET)
        nodes: list[ClusterDataNodeStatus] | Unset = UNSET
        if _nodes is not UNSET:
            nodes = []
            for nodes_item_data in _nodes:
                nodes_item = ClusterDataNodeStatus.from_dict(nodes_item_data)

                nodes.append(nodes_item)

        _ranges = d.pop("ranges", UNSET)
        ranges: list[ClusterDataRangeStatus] | Unset = UNSET
        if _ranges is not UNSET:
            ranges = []
            for ranges_item_data in _ranges:
                ranges_item = ClusterDataRangeStatus.from_dict(ranges_item_data)

                ranges.append(ranges_item)

        _replicas = d.pop("replicas", UNSET)
        replicas: list[ClusterDataReplicaStatus] | Unset = UNSET
        if _replicas is not UNSET:
            replicas = []
            for replicas_item_data in _replicas:
                replicas_item = ClusterDataReplicaStatus.from_dict(replicas_item_data)

                replicas.append(replicas_item)

        _groups = d.pop("groups", UNSET)
        groups: list[ClusterDataGroupStatus] | Unset = UNSET
        if _groups is not UNSET:
            groups = []
            for groups_item_data in _groups:
                groups_item = ClusterDataGroupStatus.from_dict(groups_item_data)

                groups.append(groups_item)

        cluster_data_status = cls(
            nodes=nodes,
            ranges=ranges,
            replicas=replicas,
            groups=groups,
        )

        cluster_data_status.additional_properties = d
        return cluster_data_status

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
