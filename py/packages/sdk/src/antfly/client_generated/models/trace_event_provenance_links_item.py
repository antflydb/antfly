from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.trace_event_provenance_links_item_relation import TraceEventProvenanceLinksItemRelation

if TYPE_CHECKING:
    from ..models.trace_event_provenance_links_item_target import TraceEventProvenanceLinksItemTarget


T = TypeVar("T", bound="TraceEventProvenanceLinksItem")


@_attrs_define
class TraceEventProvenanceLinksItem:
    """
    Attributes:
        relation (TraceEventProvenanceLinksItemRelation):
        target (TraceEventProvenanceLinksItemTarget):
    """

    relation: TraceEventProvenanceLinksItemRelation
    target: TraceEventProvenanceLinksItemTarget

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
        from ..models.trace_event_provenance_links_item_target import TraceEventProvenanceLinksItemTarget

        d = dict(src_dict)
        relation = TraceEventProvenanceLinksItemRelation(d.pop("relation"))

        target = TraceEventProvenanceLinksItemTarget.from_dict(d.pop("target"))

        trace_event_provenance_links_item = cls(
            relation=relation,
            target=target,
        )

        return trace_event_provenance_links_item
