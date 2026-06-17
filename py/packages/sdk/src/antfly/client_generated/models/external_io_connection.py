from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.external_io_protocol import ExternalIoProtocol
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExternalIoConnection")


@_attrs_define
class ExternalIoConnection:
    """
    Attributes:
        protocol (ExternalIoProtocol): External IO transport protocol.
        endpoint (str | Unset): Custom endpoint URL when configured.
        buckets (list[str] | Unset): Buckets this connection is configured for.
        prefix (str | Unset): Key prefix when configured.
        hosts (list[str] | Unset): Hosts or base URLs this connection applies to.
    """

    protocol: ExternalIoProtocol
    endpoint: str | Unset = UNSET
    buckets: list[str] | Unset = UNSET
    prefix: str | Unset = UNSET
    hosts: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        protocol = self.protocol.value

        endpoint = self.endpoint

        buckets: list[str] | Unset = UNSET
        if not isinstance(self.buckets, Unset):
            buckets = self.buckets

        prefix = self.prefix

        hosts: list[str] | Unset = UNSET
        if not isinstance(self.hosts, Unset):
            hosts = self.hosts

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "protocol": protocol,
            }
        )
        if endpoint is not UNSET:
            field_dict["endpoint"] = endpoint
        if buckets is not UNSET:
            field_dict["buckets"] = buckets
        if prefix is not UNSET:
            field_dict["prefix"] = prefix
        if hosts is not UNSET:
            field_dict["hosts"] = hosts

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        protocol = ExternalIoProtocol(d.pop("protocol"))

        endpoint = d.pop("endpoint", UNSET)

        buckets = cast(list[str], d.pop("buckets", UNSET))

        prefix = d.pop("prefix", UNSET)

        hosts = cast(list[str], d.pop("hosts", UNSET))

        external_io_connection = cls(
            protocol=protocol,
            endpoint=endpoint,
            buckets=buckets,
            prefix=prefix,
            hosts=hosts,
        )

        external_io_connection.additional_properties = d
        return external_io_connection

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
