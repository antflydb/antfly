from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_label_mapping import GraphLabelMapping
    from ..models.graph_relationship_mapping import GraphRelationshipMapping


T = TypeVar("T", bound="GraphDefinition")


@_attrs_define
class GraphDefinition:
    """
    Attributes:
        labels (list[GraphLabelMapping]):
        relationships (list[GraphRelationshipMapping] | Unset):
    """

    labels: list[GraphLabelMapping]
    relationships: list[GraphRelationshipMapping] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        labels = []
        for labels_item_data in self.labels:
            labels_item = labels_item_data.to_dict()
            labels.append(labels_item)

        relationships: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.relationships, Unset):
            relationships = []
            for relationships_item_data in self.relationships:
                relationships_item = relationships_item_data.to_dict()
                relationships.append(relationships_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "labels": labels,
            }
        )
        if relationships is not UNSET:
            field_dict["relationships"] = relationships

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_label_mapping import GraphLabelMapping
        from ..models.graph_relationship_mapping import GraphRelationshipMapping

        d = dict(src_dict)
        labels = []
        _labels = d.pop("labels")
        for labels_item_data in _labels:
            labels_item = GraphLabelMapping.from_dict(labels_item_data)

            labels.append(labels_item)

        _relationships = d.pop("relationships", UNSET)
        relationships: list[GraphRelationshipMapping] | Unset = UNSET
        if _relationships is not UNSET:
            relationships = []
            for relationships_item_data in _relationships:
                relationships_item = GraphRelationshipMapping.from_dict(relationships_item_data)

                relationships.append(relationships_item)

        graph_definition = cls(
            labels=labels,
            relationships=relationships,
        )

        return graph_definition
