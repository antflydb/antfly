from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_where_not_equal import GraphWhereNotEqual
    from ..models.graph_where_not_exists import GraphWhereNotExists


T = TypeVar("T", bound="GraphWhereAnd")


@_attrs_define
class GraphWhereAnd:
    """
    Attributes:
        and_ (list[GraphWhereAnd | GraphWhereNotEqual | GraphWhereNotExists]):
    """

    and_: list[GraphWhereAnd | GraphWhereNotEqual | GraphWhereNotExists]

    def to_dict(self) -> dict[str, Any]:
        from ..models.graph_where_not_equal import GraphWhereNotEqual

        and_ = []
        for and_item_data in self.and_:
            and_item: dict[str, Any]
            if isinstance(and_item_data, GraphWhereAnd):
                and_item = and_item_data.to_dict()
            elif isinstance(and_item_data, GraphWhereNotEqual):
                and_item = and_item_data.to_dict()
            else:
                and_item = and_item_data.to_dict()

            and_.append(and_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "and": and_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_where_not_equal import GraphWhereNotEqual
        from ..models.graph_where_not_exists import GraphWhereNotExists

        d = dict(src_dict)
        and_ = []
        _and_ = d.pop("and")
        for and_item_data in _and_:

            def _parse_and_item(data: object) -> GraphWhereAnd | GraphWhereNotEqual | GraphWhereNotExists:
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_graph_where_expression_type_0 = GraphWhereAnd.from_dict(data)

                    return componentsschemas_graph_where_expression_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_graph_where_expression_type_1 = GraphWhereNotEqual.from_dict(data)

                    return componentsschemas_graph_where_expression_type_1
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_where_expression_type_2 = GraphWhereNotExists.from_dict(data)

                return componentsschemas_graph_where_expression_type_2

            and_item = _parse_and_item(and_item_data)

            and_.append(and_item)

        graph_where_and = cls(
            and_=and_,
        )

        return graph_where_and
