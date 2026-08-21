from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_not_exists_pattern import GraphNotExistsPattern


T = TypeVar("T", bound="GraphWhereNotExists")


@_attrs_define
class GraphWhereNotExists:
    """
    Attributes:
        not_exists (GraphNotExistsPattern):
    """

    not_exists: GraphNotExistsPattern

    def to_dict(self) -> dict[str, Any]:
        not_exists = self.not_exists.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "not_exists": not_exists,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_not_exists_pattern import GraphNotExistsPattern

        d = dict(src_dict)
        not_exists = GraphNotExistsPattern.from_dict(d.pop("not_exists"))

        graph_where_not_exists = cls(
            not_exists=not_exists,
        )

        return graph_where_not_exists
