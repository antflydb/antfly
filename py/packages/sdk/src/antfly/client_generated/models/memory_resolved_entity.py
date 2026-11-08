from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_entity import MemoryEntity
    from ..models.resource_reference import ResourceReference


T = TypeVar("T", bound="MemoryResolvedEntity")


@_attrs_define
class MemoryResolvedEntity:
    """
    Attributes:
        entity (MemoryEntity):
        resource_ref (ResourceReference):
        canonical_id (str):
        requested_id (str):
        revision (int):
        identity (str):
        uncertainty (str):
    """

    entity: MemoryEntity
    resource_ref: ResourceReference
    canonical_id: str
    requested_id: str
    revision: int
    identity: str
    uncertainty: str

    def to_dict(self) -> dict[str, Any]:
        entity = self.entity.to_dict()

        resource_ref = self.resource_ref.to_dict()

        canonical_id = self.canonical_id

        requested_id = self.requested_id

        revision = self.revision

        identity = self.identity

        uncertainty = self.uncertainty

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "entity": entity,
                "resource_ref": resource_ref,
                "canonical_id": canonical_id,
                "requested_id": requested_id,
                "revision": revision,
                "identity": identity,
                "uncertainty": uncertainty,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity import MemoryEntity
        from ..models.resource_reference import ResourceReference

        d = dict(src_dict)
        entity = MemoryEntity.from_dict(d.pop("entity"))

        resource_ref = ResourceReference.from_dict(d.pop("resource_ref"))

        canonical_id = d.pop("canonical_id")

        requested_id = d.pop("requested_id")

        revision = d.pop("revision")

        identity = d.pop("identity")

        uncertainty = d.pop("uncertainty")

        memory_resolved_entity = cls(
            entity=entity,
            resource_ref=resource_ref,
            canonical_id=canonical_id,
            requested_id=requested_id,
            revision=revision,
            identity=identity,
            uncertainty=uncertainty,
        )

        return memory_resolved_entity
