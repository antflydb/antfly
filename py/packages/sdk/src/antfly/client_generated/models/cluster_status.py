from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.cluster_health import ClusterHealth
from ..models.cluster_status_deployment_mode import ClusterStatusDeploymentMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.secret_store_status import SecretStoreStatus
    from ..models.storage_runtime_status import StorageRuntimeStatus


T = TypeVar("T", bound="ClusterStatus")


@_attrs_define
class ClusterStatus:
    """
    Attributes:
        health (ClusterHealth): Overall health status of the cluster
        message (str | Unset): Optional message providing details about the health status
        auth_enabled (bool | Unset): Indicates whether authentication is enabled for the cluster
        deployment_mode (ClusterStatusDeploymentMode | Unset): Runtime deployment topology
        secret_store (SecretStoreStatus | Unset): Non-secret status for the local secrets file store, when one is
            available.
        storage (StorageRuntimeStatus | Unset):
    """

    health: ClusterHealth
    message: str | Unset = UNSET
    auth_enabled: bool | Unset = UNSET
    deployment_mode: ClusterStatusDeploymentMode | Unset = UNSET
    secret_store: SecretStoreStatus | Unset = UNSET
    storage: StorageRuntimeStatus | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        health = self.health.value

        message = self.message

        auth_enabled = self.auth_enabled

        deployment_mode: str | Unset = UNSET
        if not isinstance(self.deployment_mode, Unset):
            deployment_mode = self.deployment_mode.value

        secret_store: dict[str, Any] | Unset = UNSET
        if not isinstance(self.secret_store, Unset):
            secret_store = self.secret_store.to_dict()

        storage: dict[str, Any] | Unset = UNSET
        if not isinstance(self.storage, Unset):
            storage = self.storage.to_dict()

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
        if deployment_mode is not UNSET:
            field_dict["deployment_mode"] = deployment_mode
        if secret_store is not UNSET:
            field_dict["secret_store"] = secret_store
        if storage is not UNSET:
            field_dict["storage"] = storage

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.secret_store_status import SecretStoreStatus
        from ..models.storage_runtime_status import StorageRuntimeStatus

        d = dict(src_dict)
        health = ClusterHealth(d.pop("health"))

        message = d.pop("message", UNSET)

        auth_enabled = d.pop("auth_enabled", UNSET)

        _deployment_mode = d.pop("deployment_mode", UNSET)
        deployment_mode: ClusterStatusDeploymentMode | Unset
        if isinstance(_deployment_mode, Unset):
            deployment_mode = UNSET
        else:
            deployment_mode = ClusterStatusDeploymentMode(_deployment_mode)

        _secret_store = d.pop("secret_store", UNSET)
        secret_store: SecretStoreStatus | Unset
        if isinstance(_secret_store, Unset):
            secret_store = UNSET
        else:
            secret_store = SecretStoreStatus.from_dict(_secret_store)

        _storage = d.pop("storage", UNSET)
        storage: StorageRuntimeStatus | Unset
        if isinstance(_storage, Unset):
            storage = UNSET
        else:
            storage = StorageRuntimeStatus.from_dict(_storage)

        cluster_status = cls(
            health=health,
            message=message,
            auth_enabled=auth_enabled,
            deployment_mode=deployment_mode,
            secret_store=secret_store,
            storage=storage,
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
