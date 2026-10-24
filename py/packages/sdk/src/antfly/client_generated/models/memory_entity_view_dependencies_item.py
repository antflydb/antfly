from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_entity_view_dependencies_item_evidence_item import MemoryEntityViewDependenciesItemEvidenceItem
    from ..models.memory_entity_view_dependencies_item_resource import MemoryEntityViewDependenciesItemResource


T = TypeVar("T", bound="MemoryEntityViewDependenciesItem")


@_attrs_define
class MemoryEntityViewDependenciesItem:
    """
    Attributes:
        resource (MemoryEntityViewDependenciesItemResource):
        evidence (list[MemoryEntityViewDependenciesItemEvidenceItem]):
    """

    resource: MemoryEntityViewDependenciesItemResource
    evidence: list[MemoryEntityViewDependenciesItemEvidenceItem]

    def to_dict(self) -> dict[str, Any]:
        resource = self.resource.to_dict()

        evidence = []
        for evidence_item_data in self.evidence:
            evidence_item = evidence_item_data.to_dict()
            evidence.append(evidence_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "resource": resource,
                "evidence": evidence,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity_view_dependencies_item_evidence_item import (
            MemoryEntityViewDependenciesItemEvidenceItem,
        )
        from ..models.memory_entity_view_dependencies_item_resource import MemoryEntityViewDependenciesItemResource

        d = dict(src_dict)
        resource = MemoryEntityViewDependenciesItemResource.from_dict(d.pop("resource"))

        evidence = []
        _evidence = d.pop("evidence")
        for evidence_item_data in _evidence:
            evidence_item = MemoryEntityViewDependenciesItemEvidenceItem.from_dict(evidence_item_data)

            evidence.append(evidence_item)

        memory_entity_view_dependencies_item = cls(
            resource=resource,
            evidence=evidence,
        )

        return memory_entity_view_dependencies_item
