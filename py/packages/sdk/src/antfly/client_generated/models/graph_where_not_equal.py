from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_not_equal_predicate import GraphNotEqualPredicate


T = TypeVar("T", bound="GraphWhereNotEqual")


@_attrs_define
class GraphWhereNotEqual:
    """
    Attributes:
        not_equal (GraphNotEqualPredicate):
    """

    not_equal: GraphNotEqualPredicate

    def to_dict(self) -> dict[str, Any]:
        not_equal = self.not_equal.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "not_equal": not_equal,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_not_equal_predicate import GraphNotEqualPredicate

        d = dict(src_dict)
        not_equal = GraphNotEqualPredicate.from_dict(d.pop("not_equal"))

        graph_where_not_equal = cls(
            not_equal=not_equal,
        )

        return graph_where_not_equal
