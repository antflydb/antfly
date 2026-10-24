from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.provenance_envelope_links_item_relation import ProvenanceEnvelopeLinksItemRelation

if TYPE_CHECKING:
    from ..models.provenance_envelope_links_item_target import ProvenanceEnvelopeLinksItemTarget


T = TypeVar("T", bound="ProvenanceEnvelopeLinksItem")


@_attrs_define
class ProvenanceEnvelopeLinksItem:
    """
    Attributes:
        relation (ProvenanceEnvelopeLinksItemRelation):
        target (ProvenanceEnvelopeLinksItemTarget):
    """

    relation: ProvenanceEnvelopeLinksItemRelation
    target: ProvenanceEnvelopeLinksItemTarget

    def to_dict(self) -> dict[str, Any]:
        relation = self.relation.value

        target = self.target.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "relation": relation,
                "target": target,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.provenance_envelope_links_item_target import ProvenanceEnvelopeLinksItemTarget

        d = dict(src_dict)
        relation = ProvenanceEnvelopeLinksItemRelation(d.pop("relation"))

        target = ProvenanceEnvelopeLinksItemTarget.from_dict(d.pop("target"))

        provenance_envelope_links_item = cls(
            relation=relation,
            target=target,
        )

        return provenance_envelope_links_item
