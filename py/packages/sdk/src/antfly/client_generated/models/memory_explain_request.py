from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_explain_request_resource_type_0 import MemoryExplainRequestResourceType0


T = TypeVar("T", bound="MemoryExplainRequest")


@_attrs_define
class MemoryExplainRequest:
    """
    Attributes:
        resource (MemoryExplainRequestResourceType0 | None | Unset):
        revision (int | None | Unset):
        valid_at (int | None | Unset):
        known_at (int | None | Unset):
        max_bytes (int | Unset):  Default: 65536.
        max_nodes (int | Unset):  Default: 64.
        max_depth (int | Unset):  Default: 8.
    """

    resource: MemoryExplainRequestResourceType0 | None | Unset = UNSET
    revision: int | None | Unset = UNSET
    valid_at: int | None | Unset = UNSET
    known_at: int | None | Unset = UNSET
    max_bytes: int | Unset = 65536
    max_nodes: int | Unset = 64
    max_depth: int | Unset = 8

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_explain_request_resource_type_0 import MemoryExplainRequestResourceType0

        resource: dict[str, Any] | None | Unset
        if isinstance(self.resource, Unset):
            resource = UNSET
        elif isinstance(self.resource, MemoryExplainRequestResourceType0):
            resource = self.resource.to_dict()
        else:
            resource = self.resource

        revision: int | None | Unset
        if isinstance(self.revision, Unset):
            revision = UNSET
        else:
            revision = self.revision

        valid_at: int | None | Unset
        if isinstance(self.valid_at, Unset):
            valid_at = UNSET
        else:
            valid_at = self.valid_at

        known_at: int | None | Unset
        if isinstance(self.known_at, Unset):
            known_at = UNSET
        else:
            known_at = self.known_at

        max_bytes = self.max_bytes

        max_nodes = self.max_nodes

        max_depth = self.max_depth

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if resource is not UNSET:
            field_dict["resource"] = resource
        if revision is not UNSET:
            field_dict["revision"] = revision
        if valid_at is not UNSET:
            field_dict["valid_at"] = valid_at
        if known_at is not UNSET:
            field_dict["known_at"] = known_at
        if max_bytes is not UNSET:
            field_dict["max_bytes"] = max_bytes
        if max_nodes is not UNSET:
            field_dict["max_nodes"] = max_nodes
        if max_depth is not UNSET:
            field_dict["max_depth"] = max_depth

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_explain_request_resource_type_0 import MemoryExplainRequestResourceType0

        d = dict(src_dict)

        def _parse_resource(data: object) -> MemoryExplainRequestResourceType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                resource_type_0 = MemoryExplainRequestResourceType0.from_dict(data)

                return resource_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryExplainRequestResourceType0 | None | Unset, data)

        resource = _parse_resource(d.pop("resource", UNSET))

        def _parse_revision(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        revision = _parse_revision(d.pop("revision", UNSET))

        def _parse_valid_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        valid_at = _parse_valid_at(d.pop("valid_at", UNSET))

        def _parse_known_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        known_at = _parse_known_at(d.pop("known_at", UNSET))

        max_bytes = d.pop("max_bytes", UNSET)

        max_nodes = d.pop("max_nodes", UNSET)

        max_depth = d.pop("max_depth", UNSET)

        memory_explain_request = cls(
            resource=resource,
            revision=revision,
            valid_at=valid_at,
            known_at=known_at,
            max_bytes=max_bytes,
            max_nodes=max_nodes,
            max_depth=max_depth,
        )

        return memory_explain_request
