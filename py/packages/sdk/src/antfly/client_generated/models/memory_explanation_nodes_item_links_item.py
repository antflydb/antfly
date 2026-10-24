from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_explanation_nodes_item_links_item_relation import MemoryExplanationNodesItemLinksItemRelation
from ..models.memory_explanation_nodes_item_links_item_status import MemoryExplanationNodesItemLinksItemStatus

if TYPE_CHECKING:
    from ..models.memory_explanation_nodes_item_links_item_target_type_0 import (
        MemoryExplanationNodesItemLinksItemTargetType0,
    )


T = TypeVar("T", bound="MemoryExplanationNodesItemLinksItem")


@_attrs_define
class MemoryExplanationNodesItemLinksItem:
    """
    Attributes:
        relation (MemoryExplanationNodesItemLinksItemRelation):
        target (MemoryExplanationNodesItemLinksItemTargetType0 | None):
        status (MemoryExplanationNodesItemLinksItemStatus):
    """

    relation: MemoryExplanationNodesItemLinksItemRelation
    target: MemoryExplanationNodesItemLinksItemTargetType0 | None
    status: MemoryExplanationNodesItemLinksItemStatus

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_explanation_nodes_item_links_item_target_type_0 import (
            MemoryExplanationNodesItemLinksItemTargetType0,
        )

        relation = self.relation.value

        target: dict[str, Any] | None
        if isinstance(self.target, MemoryExplanationNodesItemLinksItemTargetType0):
            target = self.target.to_dict()
        else:
            target = self.target

        status = self.status.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "relation": relation,
                "target": target,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_explanation_nodes_item_links_item_target_type_0 import (
            MemoryExplanationNodesItemLinksItemTargetType0,
        )

        d = dict(src_dict)
        relation = MemoryExplanationNodesItemLinksItemRelation(d.pop("relation"))

        def _parse_target(data: object) -> MemoryExplanationNodesItemLinksItemTargetType0 | None:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                target_type_0 = MemoryExplanationNodesItemLinksItemTargetType0.from_dict(data)

                return target_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryExplanationNodesItemLinksItemTargetType0 | None, data)

        target = _parse_target(d.pop("target"))

        status = MemoryExplanationNodesItemLinksItemStatus(d.pop("status"))

        memory_explanation_nodes_item_links_item = cls(
            relation=relation,
            target=target,
            status=status,
        )

        return memory_explanation_nodes_item_links_item
