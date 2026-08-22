from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_bounded_traversal_config import GraphBoundedTraversalConfig


T = TypeVar("T", bound="GraphAlgebraicPlanningConfig")


@_attrs_define
class GraphAlgebraicPlanningConfig:
    """Optional algebraic planning features for graph traversal.

    Attributes:
        bounded_traversal (GraphBoundedTraversalConfig | Unset): Algebraic law used to combine bounded graph traversal
            provenance.
    """

    bounded_traversal: GraphBoundedTraversalConfig | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        bounded_traversal: dict[str, Any] | Unset = UNSET
        if not isinstance(self.bounded_traversal, Unset):
            bounded_traversal = self.bounded_traversal.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if bounded_traversal is not UNSET:
            field_dict["bounded_traversal"] = bounded_traversal

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_bounded_traversal_config import GraphBoundedTraversalConfig

        d = dict(src_dict)
        _bounded_traversal = d.pop("bounded_traversal", UNSET)
        bounded_traversal: GraphBoundedTraversalConfig | Unset
        if isinstance(_bounded_traversal, Unset):
            bounded_traversal = UNSET
        else:
            bounded_traversal = GraphBoundedTraversalConfig.from_dict(_bounded_traversal)

        graph_algebraic_planning_config = cls(
            bounded_traversal=bounded_traversal,
        )

        return graph_algebraic_planning_config
