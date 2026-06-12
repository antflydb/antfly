from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.connection_kind import ConnectionKind
from ..models.connection_status import ConnectionStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_provider_connection import InferenceProviderConnection
    from ..models.object_store_connection import ObjectStoreConnection
    from ..models.remote_content_http_connection import RemoteContentHttpConnection


T = TypeVar("T", bound="Connection")


@_attrs_define
class Connection:
    """
    Attributes:
        name (str): Stable identifier for this connection instance.
        kind (ConnectionKind): Kind of external connection configured on this node.
        status (ConnectionStatus): Connection status. "connected" means a live probe or listing succeeded,
            "error" means the probe failed (see the error field), "configured" means
            the connection is present but was not probed, and "unsupported" means
            no probe is available for this connection kind or provider.
        error (str | Unset): Failure detail when status is "error".
        sources (list[str] | Unset): Where this connection was configured, e.g.
            "config:embedders/openai-small" or "table:docs/index:body_vec".
        inference_provider (InferenceProviderConnection | Unset):
        object_store (ObjectStoreConnection | Unset):
        remote_content_http (RemoteContentHttpConnection | Unset):
    """

    name: str
    kind: ConnectionKind
    status: ConnectionStatus
    error: str | Unset = UNSET
    sources: list[str] | Unset = UNSET
    inference_provider: InferenceProviderConnection | Unset = UNSET
    object_store: ObjectStoreConnection | Unset = UNSET
    remote_content_http: RemoteContentHttpConnection | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind = self.kind.value

        status = self.status.value

        error = self.error

        sources: list[str] | Unset = UNSET
        if not isinstance(self.sources, Unset):
            sources = self.sources

        inference_provider: dict[str, Any] | Unset = UNSET
        if not isinstance(self.inference_provider, Unset):
            inference_provider = self.inference_provider.to_dict()

        object_store: dict[str, Any] | Unset = UNSET
        if not isinstance(self.object_store, Unset):
            object_store = self.object_store.to_dict()

        remote_content_http: dict[str, Any] | Unset = UNSET
        if not isinstance(self.remote_content_http, Unset):
            remote_content_http = self.remote_content_http.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "kind": kind,
                "status": status,
            }
        )
        if error is not UNSET:
            field_dict["error"] = error
        if sources is not UNSET:
            field_dict["sources"] = sources
        if inference_provider is not UNSET:
            field_dict["inference_provider"] = inference_provider
        if object_store is not UNSET:
            field_dict["object_store"] = object_store
        if remote_content_http is not UNSET:
            field_dict["remote_content_http"] = remote_content_http

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_provider_connection import InferenceProviderConnection
        from ..models.object_store_connection import ObjectStoreConnection
        from ..models.remote_content_http_connection import RemoteContentHttpConnection

        d = dict(src_dict)
        name = d.pop("name")

        kind = ConnectionKind(d.pop("kind"))

        status = ConnectionStatus(d.pop("status"))

        error = d.pop("error", UNSET)

        sources = cast(list[str], d.pop("sources", UNSET))

        _inference_provider = d.pop("inference_provider", UNSET)
        inference_provider: InferenceProviderConnection | Unset
        if isinstance(_inference_provider, Unset):
            inference_provider = UNSET
        else:
            inference_provider = InferenceProviderConnection.from_dict(_inference_provider)

        _object_store = d.pop("object_store", UNSET)
        object_store: ObjectStoreConnection | Unset
        if isinstance(_object_store, Unset):
            object_store = UNSET
        else:
            object_store = ObjectStoreConnection.from_dict(_object_store)

        _remote_content_http = d.pop("remote_content_http", UNSET)
        remote_content_http: RemoteContentHttpConnection | Unset
        if isinstance(_remote_content_http, Unset):
            remote_content_http = UNSET
        else:
            remote_content_http = RemoteContentHttpConnection.from_dict(_remote_content_http)

        connection = cls(
            name=name,
            kind=kind,
            status=status,
            error=error,
            sources=sources,
            inference_provider=inference_provider,
            object_store=object_store,
            remote_content_http=remote_content_http,
        )

        connection.additional_properties = d
        return connection

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
