from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.connection_kind import ConnectionKind
from ..models.connection_status import ConnectionStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.cdc_connection import CdcConnection
    from ..models.inference_connection import InferenceConnection
    from ..models.object_store_connection import ObjectStoreConnection
    from ..models.remote_content_connection import RemoteContentConnection


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
        inference (InferenceConnection | Unset):
        object_store (ObjectStoreConnection | Unset):
        remote_content (RemoteContentConnection | Unset):
        cdc (CdcConnection | Unset):
    """

    name: str
    kind: ConnectionKind
    status: ConnectionStatus
    error: str | Unset = UNSET
    sources: list[str] | Unset = UNSET
    inference: InferenceConnection | Unset = UNSET
    object_store: ObjectStoreConnection | Unset = UNSET
    remote_content: RemoteContentConnection | Unset = UNSET
    cdc: CdcConnection | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind = self.kind.value

        status = self.status.value

        error = self.error

        sources: list[str] | Unset = UNSET
        if not isinstance(self.sources, Unset):
            sources = self.sources

        inference: dict[str, Any] | Unset = UNSET
        if not isinstance(self.inference, Unset):
            inference = self.inference.to_dict()

        object_store: dict[str, Any] | Unset = UNSET
        if not isinstance(self.object_store, Unset):
            object_store = self.object_store.to_dict()

        remote_content: dict[str, Any] | Unset = UNSET
        if not isinstance(self.remote_content, Unset):
            remote_content = self.remote_content.to_dict()

        cdc: dict[str, Any] | Unset = UNSET
        if not isinstance(self.cdc, Unset):
            cdc = self.cdc.to_dict()

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
        if inference is not UNSET:
            field_dict["inference"] = inference
        if object_store is not UNSET:
            field_dict["object_store"] = object_store
        if remote_content is not UNSET:
            field_dict["remote_content"] = remote_content
        if cdc is not UNSET:
            field_dict["cdc"] = cdc

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.cdc_connection import CdcConnection
        from ..models.inference_connection import InferenceConnection
        from ..models.object_store_connection import ObjectStoreConnection
        from ..models.remote_content_connection import RemoteContentConnection

        d = dict(src_dict)
        name = d.pop("name")

        kind = ConnectionKind(d.pop("kind"))

        status = ConnectionStatus(d.pop("status"))

        error = d.pop("error", UNSET)

        sources = cast(list[str], d.pop("sources", UNSET))

        _inference = d.pop("inference", UNSET)
        inference: InferenceConnection | Unset
        if isinstance(_inference, Unset):
            inference = UNSET
        else:
            inference = InferenceConnection.from_dict(_inference)

        _object_store = d.pop("object_store", UNSET)
        object_store: ObjectStoreConnection | Unset
        if isinstance(_object_store, Unset):
            object_store = UNSET
        else:
            object_store = ObjectStoreConnection.from_dict(_object_store)

        _remote_content = d.pop("remote_content", UNSET)
        remote_content: RemoteContentConnection | Unset
        if isinstance(_remote_content, Unset):
            remote_content = UNSET
        else:
            remote_content = RemoteContentConnection.from_dict(_remote_content)

        _cdc = d.pop("cdc", UNSET)
        cdc: CdcConnection | Unset
        if isinstance(_cdc, Unset):
            cdc = UNSET
        else:
            cdc = CdcConnection.from_dict(_cdc)

        connection = cls(
            name=name,
            kind=kind,
            status=status,
            error=error,
            sources=sources,
            inference=inference,
            object_store=object_store,
            remote_content=remote_content,
            cdc=cdc,
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
