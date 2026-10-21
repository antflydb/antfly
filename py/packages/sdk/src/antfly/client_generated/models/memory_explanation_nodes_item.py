from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_explanation_nodes_item_links_item import MemoryExplanationNodesItemLinksItem
    from ..models.memory_explanation_nodes_item_resource import MemoryExplanationNodesItemResource


T = TypeVar("T", bound="MemoryExplanationNodesItem")


@_attrs_define
class MemoryExplanationNodesItem:
    """
    Attributes:
        resource (MemoryExplanationNodesItemResource):
        record (Any):
        links (list[MemoryExplanationNodesItemLinksItem]):
        payload_truncated (bool | Unset):
    """

    resource: MemoryExplanationNodesItemResource
    record: Any
    links: list[MemoryExplanationNodesItemLinksItem]
    payload_truncated: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        resource = self.resource.to_dict()

        record = self.record

        links = []
        for links_item_data in self.links:
            links_item = links_item_data.to_dict()
            links.append(links_item)

        payload_truncated = self.payload_truncated

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "resource": resource,
                "record": record,
                "links": links,
            }
        )
        if payload_truncated is not UNSET:
            field_dict["payload_truncated"] = payload_truncated

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_explanation_nodes_item_links_item import MemoryExplanationNodesItemLinksItem
        from ..models.memory_explanation_nodes_item_resource import MemoryExplanationNodesItemResource

        d = dict(src_dict)
        resource = MemoryExplanationNodesItemResource.from_dict(d.pop("resource"))

        record = d.pop("record")

        links = []
        _links = d.pop("links")
        for links_item_data in _links:
            links_item = MemoryExplanationNodesItemLinksItem.from_dict(links_item_data)

            links.append(links_item)

        payload_truncated = d.pop("payload_truncated", UNSET)

        memory_explanation_nodes_item = cls(
            resource=resource,
            record=record,
            links=links,
            payload_truncated=payload_truncated,
        )

        return memory_explanation_nodes_item
