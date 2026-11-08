from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphRelationshipMapping")


@_attrs_define
class GraphRelationshipMapping:
    """
    Attributes:
        name (str):
        index (str):
        source_label (str):
        target_label (str):
        stored_type (str):
    """

    name: str
    index: str
    source_label: str
    target_label: str
    stored_type: str

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        index = self.index

        source_label = self.source_label

        target_label = self.target_label

        stored_type = self.stored_type

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "index": index,
                "source_label": source_label,
                "target_label": target_label,
                "stored_type": stored_type,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        index = d.pop("index")

        source_label = d.pop("source_label")

        target_label = d.pop("target_label")

        stored_type = d.pop("stored_type")

        graph_relationship_mapping = cls(
            name=name,
            index=index,
            source_label=source_label,
            target_label=target_label,
            stored_type=stored_type,
        )

        return graph_relationship_mapping
