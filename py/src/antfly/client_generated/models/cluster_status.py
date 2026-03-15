from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.cluster_health import ClusterHealth
from ..types import UNSET, Unset

T = TypeVar("T", bound="ClusterStatus")


@_attrs_define
class ClusterStatus:
    """
    Attributes:
        health (ClusterHealth): Overall health status of the cluster
        message (Union[Unset, str]): Optional message providing details about the health status
        auth_enabled (Union[Unset, bool]): Indicates whether authentication is enabled for the cluster
        swarm_mode (Union[Unset, bool]): Indicates whether the cluster is running in single-node swarm mode
    """

    health: ClusterHealth
    message: Union[Unset, str] = UNSET
    auth_enabled: Union[Unset, bool] = UNSET
    swarm_mode: Union[Unset, bool] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        health = self.health.value

        message = self.message

        auth_enabled = self.auth_enabled

        swarm_mode = self.swarm_mode

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "health": health,
            }
        )
        if message is not UNSET:
            field_dict["message"] = message
        if auth_enabled is not UNSET:
            field_dict["auth_enabled"] = auth_enabled
        if swarm_mode is not UNSET:
            field_dict["swarm_mode"] = swarm_mode

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        health = ClusterHealth(d.pop("health"))

        message = d.pop("message", UNSET)

        auth_enabled = d.pop("auth_enabled", UNSET)

        swarm_mode = d.pop("swarm_mode", UNSET)

        cluster_status = cls(
            health=health,
            message=message,
            auth_enabled=auth_enabled,
            swarm_mode=swarm_mode,
        )

        cluster_status.additional_properties = d
        return cluster_status

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
