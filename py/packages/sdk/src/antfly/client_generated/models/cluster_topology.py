from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.cluster_health import ClusterHealth
from ..models.cluster_topology_deployment_mode import ClusterTopologyDeploymentMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.cluster_data_status import ClusterDataStatus
    from ..models.index_runtime_capabilities import IndexRuntimeCapabilities
    from ..models.runtime_config_status import RuntimeConfigStatus
    from ..models.secret_store_status import SecretStoreStatus
    from ..models.storage_runtime_status import StorageRuntimeStatus


T = TypeVar("T", bound="ClusterTopology")


@_attrs_define
class ClusterTopology:
    """
    Attributes:
        health (ClusterHealth): Overall health status of the cluster
        data (ClusterDataStatus): Typed Zig status view for table data topology and range placement.
        message (str | Unset): Optional message providing details about the health status
        auth_enabled (bool | Unset): Indicates whether authentication is enabled for the cluster
        deployment_mode (ClusterTopologyDeploymentMode | Unset): Runtime deployment topology
        index_capabilities (IndexRuntimeCapabilities | Unset): Deployment-level index capabilities clients can inspect
            before submitting index mutations.
        secret_store (SecretStoreStatus | Unset): Non-secret status for the local secrets file store, when one is
            available.
        runtime_config (RuntimeConfigStatus | Unset): Non-secret status for the applied config.json snapshot. Hot
            publication accepts validated remote_content-only changes; startup-only changes remain stale until restart.
        storage (StorageRuntimeStatus | Unset):
    """

    health: ClusterHealth
    data: ClusterDataStatus
    message: str | Unset = UNSET
    auth_enabled: bool | Unset = UNSET
    deployment_mode: ClusterTopologyDeploymentMode | Unset = UNSET
    index_capabilities: IndexRuntimeCapabilities | Unset = UNSET
    secret_store: SecretStoreStatus | Unset = UNSET
    runtime_config: RuntimeConfigStatus | Unset = UNSET
    storage: StorageRuntimeStatus | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        health = self.health.value

        data = self.data.to_dict()

        message = self.message

        auth_enabled = self.auth_enabled

        deployment_mode: str | Unset = UNSET
        if not isinstance(self.deployment_mode, Unset):
            deployment_mode = self.deployment_mode.value

        index_capabilities: dict[str, Any] | Unset = UNSET
        if not isinstance(self.index_capabilities, Unset):
            index_capabilities = self.index_capabilities.to_dict()

        secret_store: dict[str, Any] | Unset = UNSET
        if not isinstance(self.secret_store, Unset):
            secret_store = self.secret_store.to_dict()

        runtime_config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.runtime_config, Unset):
            runtime_config = self.runtime_config.to_dict()

        storage: dict[str, Any] | Unset = UNSET
        if not isinstance(self.storage, Unset):
            storage = self.storage.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "health": health,
                "data": data,
            }
        )
        if message is not UNSET:
            field_dict["message"] = message
        if auth_enabled is not UNSET:
            field_dict["auth_enabled"] = auth_enabled
        if deployment_mode is not UNSET:
            field_dict["deployment_mode"] = deployment_mode
        if index_capabilities is not UNSET:
            field_dict["index_capabilities"] = index_capabilities
        if secret_store is not UNSET:
            field_dict["secret_store"] = secret_store
        if runtime_config is not UNSET:
            field_dict["runtime_config"] = runtime_config
        if storage is not UNSET:
            field_dict["storage"] = storage

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.cluster_data_status import ClusterDataStatus
        from ..models.index_runtime_capabilities import IndexRuntimeCapabilities
        from ..models.runtime_config_status import RuntimeConfigStatus
        from ..models.secret_store_status import SecretStoreStatus
        from ..models.storage_runtime_status import StorageRuntimeStatus

        d = dict(src_dict)
        health = ClusterHealth(d.pop("health"))

        data = ClusterDataStatus.from_dict(d.pop("data"))

        message = d.pop("message", UNSET)

        auth_enabled = d.pop("auth_enabled", UNSET)

        _deployment_mode = d.pop("deployment_mode", UNSET)
        deployment_mode: ClusterTopologyDeploymentMode | Unset
        if isinstance(_deployment_mode, Unset):
            deployment_mode = UNSET
        else:
            deployment_mode = ClusterTopologyDeploymentMode(_deployment_mode)

        _index_capabilities = d.pop("index_capabilities", UNSET)
        index_capabilities: IndexRuntimeCapabilities | Unset
        if isinstance(_index_capabilities, Unset):
            index_capabilities = UNSET
        else:
            index_capabilities = IndexRuntimeCapabilities.from_dict(_index_capabilities)

        _secret_store = d.pop("secret_store", UNSET)
        secret_store: SecretStoreStatus | Unset
        if isinstance(_secret_store, Unset):
            secret_store = UNSET
        else:
            secret_store = SecretStoreStatus.from_dict(_secret_store)

        _runtime_config = d.pop("runtime_config", UNSET)
        runtime_config: RuntimeConfigStatus | Unset
        if isinstance(_runtime_config, Unset):
            runtime_config = UNSET
        else:
            runtime_config = RuntimeConfigStatus.from_dict(_runtime_config)

        _storage = d.pop("storage", UNSET)
        storage: StorageRuntimeStatus | Unset
        if isinstance(_storage, Unset):
            storage = UNSET
        else:
            storage = StorageRuntimeStatus.from_dict(_storage)

        cluster_topology = cls(
            health=health,
            data=data,
            message=message,
            auth_enabled=auth_enabled,
            deployment_mode=deployment_mode,
            index_capabilities=index_capabilities,
            secret_store=secret_store,
            runtime_config=runtime_config,
            storage=storage,
        )

        cluster_topology.additional_properties = d
        return cluster_topology

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
