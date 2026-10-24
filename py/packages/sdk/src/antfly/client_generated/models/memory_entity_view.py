from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_entity_view_dependencies_item import MemoryEntityViewDependenciesItem
    from ..models.memory_entity_view_entity import MemoryEntityViewEntity


T = TypeVar("T", bound="MemoryEntityView")


@_attrs_define
class MemoryEntityView:
    """
    Attributes:
        entity (MemoryEntityViewEntity):
        canonical_id (str):
        description (str):
        dependencies (list[MemoryEntityViewDependenciesItem]):
        fingerprint (str):
        revision (int):
        config_generation (int):
        complete (bool):
        scanned (int):
    """

    entity: MemoryEntityViewEntity
    canonical_id: str
    description: str
    dependencies: list[MemoryEntityViewDependenciesItem]
    fingerprint: str
    revision: int
    config_generation: int
    complete: bool
    scanned: int

    def to_dict(self) -> dict[str, Any]:
        entity = self.entity.to_dict()

        canonical_id = self.canonical_id

        description = self.description

        dependencies = []
        for dependencies_item_data in self.dependencies:
            dependencies_item = dependencies_item_data.to_dict()
            dependencies.append(dependencies_item)

        fingerprint = self.fingerprint

        revision = self.revision

        config_generation = self.config_generation

        complete = self.complete

        scanned = self.scanned

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "entity": entity,
                "canonical_id": canonical_id,
                "description": description,
                "dependencies": dependencies,
                "fingerprint": fingerprint,
                "revision": revision,
                "config_generation": config_generation,
                "complete": complete,
                "scanned": scanned,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity_view_dependencies_item import MemoryEntityViewDependenciesItem
        from ..models.memory_entity_view_entity import MemoryEntityViewEntity

        d = dict(src_dict)
        entity = MemoryEntityViewEntity.from_dict(d.pop("entity"))

        canonical_id = d.pop("canonical_id")

        description = d.pop("description")

        dependencies = []
        _dependencies = d.pop("dependencies")
        for dependencies_item_data in _dependencies:
            dependencies_item = MemoryEntityViewDependenciesItem.from_dict(dependencies_item_data)

            dependencies.append(dependencies_item)

        fingerprint = d.pop("fingerprint")

        revision = d.pop("revision")

        config_generation = d.pop("config_generation")

        complete = d.pop("complete")

        scanned = d.pop("scanned")

        memory_entity_view = cls(
            entity=entity,
            canonical_id=canonical_id,
            description=description,
            dependencies=dependencies,
            fingerprint=fingerprint,
            revision=revision,
            config_generation=config_generation,
            complete=complete,
            scanned=scanned,
        )

        return memory_entity_view
