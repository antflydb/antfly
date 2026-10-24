from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_recall_result_entities_item_dependencies_item import (
        MemoryRecallResultEntitiesItemDependenciesItem,
    )
    from ..models.memory_recall_result_entities_item_entity import MemoryRecallResultEntitiesItemEntity


T = TypeVar("T", bound="MemoryRecallResultEntitiesItem")


@_attrs_define
class MemoryRecallResultEntitiesItem:
    """
    Attributes:
        entity (MemoryRecallResultEntitiesItemEntity):
        canonical_id (str):
        description (str):
        dependencies (list[MemoryRecallResultEntitiesItemDependenciesItem]):
        fingerprint (str):
        revision (int):
        config_generation (int):
        complete (bool):
        scanned (int):
    """

    entity: MemoryRecallResultEntitiesItemEntity
    canonical_id: str
    description: str
    dependencies: list[MemoryRecallResultEntitiesItemDependenciesItem]
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
        from ..models.memory_recall_result_entities_item_dependencies_item import (
            MemoryRecallResultEntitiesItemDependenciesItem,
        )
        from ..models.memory_recall_result_entities_item_entity import MemoryRecallResultEntitiesItemEntity

        d = dict(src_dict)
        entity = MemoryRecallResultEntitiesItemEntity.from_dict(d.pop("entity"))

        canonical_id = d.pop("canonical_id")

        description = d.pop("description")

        dependencies = []
        _dependencies = d.pop("dependencies")
        for dependencies_item_data in _dependencies:
            dependencies_item = MemoryRecallResultEntitiesItemDependenciesItem.from_dict(dependencies_item_data)

            dependencies.append(dependencies_item)

        fingerprint = d.pop("fingerprint")

        revision = d.pop("revision")

        config_generation = d.pop("config_generation")

        complete = d.pop("complete")

        scanned = d.pop("scanned")

        memory_recall_result_entities_item = cls(
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

        return memory_recall_result_entities_item
