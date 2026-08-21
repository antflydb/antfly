from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_bounded_traversal_config_law import GraphBoundedTraversalConfigLaw
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphBoundedTraversalConfig")


@_attrs_define
class GraphBoundedTraversalConfig:
    """Algebraic law used to combine bounded graph traversal provenance.

    Attributes:
        law (GraphBoundedTraversalConfigLaw):
        enabled (bool | Unset):  Default: True.
    """

    law: GraphBoundedTraversalConfigLaw
    enabled: bool | Unset = True

    def to_dict(self) -> dict[str, Any]:
        law = self.law.value

        enabled = self.enabled

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "law": law,
            }
        )
        if enabled is not UNSET:
            field_dict["enabled"] = enabled

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        law = GraphBoundedTraversalConfigLaw(d.pop("law"))

        enabled = d.pop("enabled", UNSET)

        graph_bounded_traversal_config = cls(
            law=law,
            enabled=enabled,
        )

        return graph_bounded_traversal_config
