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
    from ..models.external_io_connection import ExternalIoConnection
    from ..models.inference_connection import InferenceConnection
    from ..models.web_search_connection import WebSearchConnection


T = TypeVar("T", bound="Connection")


@_attrs_define
class Connection:
    """
    Attributes:
        id (str): Stable resource identifier for this connection. Config-derived connections synthesize this from the
            source config path.
        name (str): Human-readable short name for this connection instance.
        kind (ConnectionKind): Kind of external connection configured on this node.
        status (ConnectionStatus): Connection status. "connected" means a live probe or listing succeeded,
            "error" means the probe failed (see the error field), "configured" means
            the connection is present but was not probed, and "unsupported" means
            no probe is available for this connection kind or provider. For S3,
            connected means an authenticated HeadBucket request succeeded for every
            explicitly allowlisted bucket. It verifies bucket discovery permission,
            not mutation permissions such as PutObject.
        capabilities (list[str]): Namespaced actions and workflow uses this connection supports, such as models.embed,
            content.fetch, objects.read, or cdc.read_stream.
        display_name (str | Unset): Optional display name for UIs.
        provider (str | Unset): Provider token for connection kinds that have a provider-level service identity, such as
            web_search.
        error (str | Unset): Failure detail when status is "error".
        sources (list[str] | Unset): Where this connection was configured, e.g.
            "config:embedders/openai-small" or "table:docs/index:body_vec".
        inference (InferenceConnection | Unset):
        web_search (WebSearchConnection | Unset):
        external_io (ExternalIoConnection | Unset):
        cdc (CdcConnection | Unset):
    """

    id: str
    name: str
    kind: ConnectionKind
    status: ConnectionStatus
    capabilities: list[str]
    display_name: str | Unset = UNSET
    provider: str | Unset = UNSET
    error: str | Unset = UNSET
    sources: list[str] | Unset = UNSET
    inference: InferenceConnection | Unset = UNSET
    web_search: WebSearchConnection | Unset = UNSET
    external_io: ExternalIoConnection | Unset = UNSET
    cdc: CdcConnection | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        name = self.name

        kind = self.kind.value

        status = self.status.value

        capabilities = self.capabilities

        display_name = self.display_name

        provider = self.provider

        error = self.error

        sources: list[str] | Unset = UNSET
        if not isinstance(self.sources, Unset):
            sources = self.sources

        inference: dict[str, Any] | Unset = UNSET
        if not isinstance(self.inference, Unset):
            inference = self.inference.to_dict()

        web_search: dict[str, Any] | Unset = UNSET
        if not isinstance(self.web_search, Unset):
            web_search = self.web_search.to_dict()

        external_io: dict[str, Any] | Unset = UNSET
        if not isinstance(self.external_io, Unset):
            external_io = self.external_io.to_dict()

        cdc: dict[str, Any] | Unset = UNSET
        if not isinstance(self.cdc, Unset):
            cdc = self.cdc.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "name": name,
                "kind": kind,
                "status": status,
                "capabilities": capabilities,
            }
        )
        if display_name is not UNSET:
            field_dict["display_name"] = display_name
        if provider is not UNSET:
            field_dict["provider"] = provider
        if error is not UNSET:
            field_dict["error"] = error
        if sources is not UNSET:
            field_dict["sources"] = sources
        if inference is not UNSET:
            field_dict["inference"] = inference
        if web_search is not UNSET:
            field_dict["web_search"] = web_search
        if external_io is not UNSET:
            field_dict["external_io"] = external_io
        if cdc is not UNSET:
            field_dict["cdc"] = cdc

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.cdc_connection import CdcConnection
        from ..models.external_io_connection import ExternalIoConnection
        from ..models.inference_connection import InferenceConnection
        from ..models.web_search_connection import WebSearchConnection

        d = dict(src_dict)
        id = d.pop("id")

        name = d.pop("name")

        kind = ConnectionKind(d.pop("kind"))

        status = ConnectionStatus(d.pop("status"))

        capabilities = cast(list[str], d.pop("capabilities"))

        display_name = d.pop("display_name", UNSET)

        provider = d.pop("provider", UNSET)

        error = d.pop("error", UNSET)

        sources = cast(list[str], d.pop("sources", UNSET))

        _inference = d.pop("inference", UNSET)
        inference: InferenceConnection | Unset
        if isinstance(_inference, Unset):
            inference = UNSET
        else:
            inference = InferenceConnection.from_dict(_inference)

        _web_search = d.pop("web_search", UNSET)
        web_search: WebSearchConnection | Unset
        if isinstance(_web_search, Unset):
            web_search = UNSET
        else:
            web_search = WebSearchConnection.from_dict(_web_search)

        _external_io = d.pop("external_io", UNSET)
        external_io: ExternalIoConnection | Unset
        if isinstance(_external_io, Unset):
            external_io = UNSET
        else:
            external_io = ExternalIoConnection.from_dict(_external_io)

        _cdc = d.pop("cdc", UNSET)
        cdc: CdcConnection | Unset
        if isinstance(_cdc, Unset):
            cdc = UNSET
        else:
            cdc = CdcConnection.from_dict(_cdc)

        connection = cls(
            id=id,
            name=name,
            kind=kind,
            status=status,
            capabilities=capabilities,
            display_name=display_name,
            provider=provider,
            error=error,
            sources=sources,
            inference=inference,
            web_search=web_search,
            external_io=external_io,
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
