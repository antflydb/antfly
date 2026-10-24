from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_entity_proposal_inputs_views_type_0_item_dependencies_item_evidence_item import (
        MemoryEntityProposalInputsViewsType0ItemDependenciesItemEvidenceItem,
    )
    from ..models.memory_entity_proposal_inputs_views_type_0_item_dependencies_item_resource import (
        MemoryEntityProposalInputsViewsType0ItemDependenciesItemResource,
    )


T = TypeVar("T", bound="MemoryEntityProposalInputsViewsType0ItemDependenciesItem")


@_attrs_define
class MemoryEntityProposalInputsViewsType0ItemDependenciesItem:
    """
    Attributes:
        resource (MemoryEntityProposalInputsViewsType0ItemDependenciesItemResource):
        evidence (list[MemoryEntityProposalInputsViewsType0ItemDependenciesItemEvidenceItem]):
    """

    resource: MemoryEntityProposalInputsViewsType0ItemDependenciesItemResource
    evidence: list[MemoryEntityProposalInputsViewsType0ItemDependenciesItemEvidenceItem]

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
        from ..models.memory_entity_proposal_inputs_views_type_0_item_dependencies_item_evidence_item import (
            MemoryEntityProposalInputsViewsType0ItemDependenciesItemEvidenceItem,
        )
        from ..models.memory_entity_proposal_inputs_views_type_0_item_dependencies_item_resource import (
            MemoryEntityProposalInputsViewsType0ItemDependenciesItemResource,
        )

        d = dict(src_dict)
        resource = MemoryEntityProposalInputsViewsType0ItemDependenciesItemResource.from_dict(d.pop("resource"))

        evidence = []
        _evidence = d.pop("evidence")
        for evidence_item_data in _evidence:
            evidence_item = MemoryEntityProposalInputsViewsType0ItemDependenciesItemEvidenceItem.from_dict(
                evidence_item_data
            )

            evidence.append(evidence_item)

        memory_entity_proposal_inputs_views_type_0_item_dependencies_item = cls(
            resource=resource,
            evidence=evidence,
        )

        return memory_entity_proposal_inputs_views_type_0_item_dependencies_item
