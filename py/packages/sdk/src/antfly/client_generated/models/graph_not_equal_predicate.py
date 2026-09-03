from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_alias_operand import GraphAliasOperand


T = TypeVar("T", bound="GraphNotEqualPredicate")


@_attrs_define
class GraphNotEqualPredicate:
    """
    Attributes:
        left (GraphAliasOperand):
        right (GraphAliasOperand):
    """

    left: GraphAliasOperand
    right: GraphAliasOperand

    def to_dict(self) -> dict[str, Any]:
        left = self.left.to_dict()

        right = self.right.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "left": left,
                "right": right,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_alias_operand import GraphAliasOperand

        d = dict(src_dict)
        left = GraphAliasOperand.from_dict(d.pop("left"))

        right = GraphAliasOperand.from_dict(d.pop("right"))

        graph_not_equal_predicate = cls(
            left=left,
            right=right,
        )

        return graph_not_equal_predicate
