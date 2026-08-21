from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_bounded_traversal_config_law import GraphBoundedTraversalConfigLaw

T = TypeVar("T", bound="GraphBoundedTraversalConfig")


@_attrs_define
class GraphBoundedTraversalConfig:
    """Algebraic law used to combine bounded graph traversal provenance.

    Attributes:
        law (GraphBoundedTraversalConfigLaw):
    """

    law: GraphBoundedTraversalConfigLaw

    def to_dict(self) -> dict[str, Any]:
        law = self.law.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "law": law,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        law = GraphBoundedTraversalConfigLaw(d.pop("law"))

        graph_bounded_traversal_config = cls(
            law=law,
        )

        return graph_bounded_traversal_config
