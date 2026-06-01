from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ClusterDataNodeStatus")


@_attrs_define
class ClusterDataNodeStatus:
    """
    Attributes:
        data_id (int):
        node_id (int):
        api_url (str | Unset):
        raft_url (str | Unset):
        role (str | Unset):
        state (str | Unset):
        health_class (str | Unset):
        failure_domain (str | Unset):
        live (bool | Unset):
        drain_requested (bool | Unset):
        capacity_bytes (int | Unset):
        available_bytes (int | Unset):
        lease_pressure (int | Unset):
        read_load (int | Unset):
        write_load (int | Unset):
        active_backfills (int | Unset):
    """

    data_id: int
    node_id: int
    api_url: str | Unset = UNSET
    raft_url: str | Unset = UNSET
    role: str | Unset = UNSET
    state: str | Unset = UNSET
    health_class: str | Unset = UNSET
    failure_domain: str | Unset = UNSET
    live: bool | Unset = UNSET
    drain_requested: bool | Unset = UNSET
    capacity_bytes: int | Unset = UNSET
    available_bytes: int | Unset = UNSET
    lease_pressure: int | Unset = UNSET
    read_load: int | Unset = UNSET
    write_load: int | Unset = UNSET
    active_backfills: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data_id = self.data_id

        node_id = self.node_id

        api_url = self.api_url

        raft_url = self.raft_url

        role = self.role

        state = self.state

        health_class = self.health_class

        failure_domain = self.failure_domain

        live = self.live

        drain_requested = self.drain_requested

        capacity_bytes = self.capacity_bytes

        available_bytes = self.available_bytes

        lease_pressure = self.lease_pressure

        read_load = self.read_load

        write_load = self.write_load

        active_backfills = self.active_backfills

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "data_id": data_id,
                "node_id": node_id,
            }
        )
        if api_url is not UNSET:
            field_dict["api_url"] = api_url
        if raft_url is not UNSET:
            field_dict["raft_url"] = raft_url
        if role is not UNSET:
            field_dict["role"] = role
        if state is not UNSET:
            field_dict["state"] = state
        if health_class is not UNSET:
            field_dict["health_class"] = health_class
        if failure_domain is not UNSET:
            field_dict["failure_domain"] = failure_domain
        if live is not UNSET:
            field_dict["live"] = live
        if drain_requested is not UNSET:
            field_dict["drain_requested"] = drain_requested
        if capacity_bytes is not UNSET:
            field_dict["capacity_bytes"] = capacity_bytes
        if available_bytes is not UNSET:
            field_dict["available_bytes"] = available_bytes
        if lease_pressure is not UNSET:
            field_dict["lease_pressure"] = lease_pressure
        if read_load is not UNSET:
            field_dict["read_load"] = read_load
        if write_load is not UNSET:
            field_dict["write_load"] = write_load
        if active_backfills is not UNSET:
            field_dict["active_backfills"] = active_backfills

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        data_id = d.pop("data_id")

        node_id = d.pop("node_id")

        api_url = d.pop("api_url", UNSET)

        raft_url = d.pop("raft_url", UNSET)

        role = d.pop("role", UNSET)

        state = d.pop("state", UNSET)

        health_class = d.pop("health_class", UNSET)

        failure_domain = d.pop("failure_domain", UNSET)

        live = d.pop("live", UNSET)

        drain_requested = d.pop("drain_requested", UNSET)

        capacity_bytes = d.pop("capacity_bytes", UNSET)

        available_bytes = d.pop("available_bytes", UNSET)

        lease_pressure = d.pop("lease_pressure", UNSET)

        read_load = d.pop("read_load", UNSET)

        write_load = d.pop("write_load", UNSET)

        active_backfills = d.pop("active_backfills", UNSET)

        cluster_data_node_status = cls(
            data_id=data_id,
            node_id=node_id,
            api_url=api_url,
            raft_url=raft_url,
            role=role,
            state=state,
            health_class=health_class,
            failure_domain=failure_domain,
            live=live,
            drain_requested=drain_requested,
            capacity_bytes=capacity_bytes,
            available_bytes=available_bytes,
            lease_pressure=lease_pressure,
            read_load=read_load,
            write_load=write_load,
            active_backfills=active_backfills,
        )

        cluster_data_node_status.additional_properties = d
        return cluster_data_node_status

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
