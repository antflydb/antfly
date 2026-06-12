from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.object_store_connection_backend import ObjectStoreConnectionBackend
from ..models.object_store_connection_purpose import ObjectStoreConnectionPurpose
from ..types import UNSET, Unset

T = TypeVar("T", bound="ObjectStoreConnection")


@_attrs_define
class ObjectStoreConnection:
    """
    Attributes:
        backend (ObjectStoreConnectionBackend): Object store backend type.
        endpoint (str | Unset): Custom endpoint URL when configured.
        buckets (list[str] | Unset): Buckets this connection is configured for.
        prefix (str | Unset): Key prefix when configured.
        purpose (ObjectStoreConnectionPurpose | Unset): What this object store is used for.
    """

    backend: ObjectStoreConnectionBackend
    endpoint: str | Unset = UNSET
    buckets: list[str] | Unset = UNSET
    prefix: str | Unset = UNSET
    purpose: ObjectStoreConnectionPurpose | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        backend = self.backend.value

        endpoint = self.endpoint

        buckets: list[str] | Unset = UNSET
        if not isinstance(self.buckets, Unset):
            buckets = self.buckets

        prefix = self.prefix

        purpose: str | Unset = UNSET
        if not isinstance(self.purpose, Unset):
            purpose = self.purpose.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "backend": backend,
            }
        )
        if endpoint is not UNSET:
            field_dict["endpoint"] = endpoint
        if buckets is not UNSET:
            field_dict["buckets"] = buckets
        if prefix is not UNSET:
            field_dict["prefix"] = prefix
        if purpose is not UNSET:
            field_dict["purpose"] = purpose

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        backend = ObjectStoreConnectionBackend(d.pop("backend"))

        endpoint = d.pop("endpoint", UNSET)

        buckets = cast(list[str], d.pop("buckets", UNSET))

        prefix = d.pop("prefix", UNSET)

        _purpose = d.pop("purpose", UNSET)
        purpose: ObjectStoreConnectionPurpose | Unset
        if isinstance(_purpose, Unset):
            purpose = UNSET
        else:
            purpose = ObjectStoreConnectionPurpose(_purpose)

        object_store_connection = cls(
            backend=backend,
            endpoint=endpoint,
            buckets=buckets,
            prefix=prefix,
            purpose=purpose,
        )

        object_store_connection.additional_properties = d
        return object_store_connection

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
