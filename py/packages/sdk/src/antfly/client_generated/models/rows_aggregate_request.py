from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_aggregate_having import RowsAggregateHaving
    from ..models.rows_aggregate_spec import RowsAggregateSpec
    from ..models.rows_query_order import RowsQueryOrder
    from ..models.rows_query_request import RowsQueryRequest


T = TypeVar("T", bound="RowsAggregateRequest")


@_attrs_define
class RowsAggregateRequest:
    """
    Attributes:
        source (RowsQueryRequest): Typed relational row-query plan. Predicate and expression arrays carry
            Antfly row-expression AST nodes; SQL syntax is adapter sugar over this
            native request shape.
        group_by (list[str] | Unset):
        aggregations (list[RowsAggregateSpec] | Unset):
        having (RowsAggregateHaving | Unset): Conjunction of aggregate-output predicates for HAVING.
        order_by (list[RowsQueryOrder] | Unset):
        limit (int | Unset):
        offset (int | Unset):
    """

    source: RowsQueryRequest
    group_by: list[str] | Unset = UNSET
    aggregations: list[RowsAggregateSpec] | Unset = UNSET
    having: RowsAggregateHaving | Unset = UNSET
    order_by: list[RowsQueryOrder] | Unset = UNSET
    limit: int | Unset = UNSET
    offset: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        source = self.source.to_dict()

        group_by: list[str] | Unset = UNSET
        if not isinstance(self.group_by, Unset):
            group_by = self.group_by

        aggregations: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.aggregations, Unset):
            aggregations = []
            for aggregations_item_data in self.aggregations:
                aggregations_item = aggregations_item_data.to_dict()
                aggregations.append(aggregations_item)

        having: dict[str, Any] | Unset = UNSET
        if not isinstance(self.having, Unset):
            having = self.having.to_dict()

        order_by: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.order_by, Unset):
            order_by = []
            for order_by_item_data in self.order_by:
                order_by_item = order_by_item_data.to_dict()
                order_by.append(order_by_item)

        limit = self.limit

        offset = self.offset

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "source": source,
            }
        )
        if group_by is not UNSET:
            field_dict["group_by"] = group_by
        if aggregations is not UNSET:
            field_dict["aggregations"] = aggregations
        if having is not UNSET:
            field_dict["having"] = having
        if order_by is not UNSET:
            field_dict["order_by"] = order_by
        if limit is not UNSET:
            field_dict["limit"] = limit
        if offset is not UNSET:
            field_dict["offset"] = offset

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_aggregate_having import RowsAggregateHaving
        from ..models.rows_aggregate_spec import RowsAggregateSpec
        from ..models.rows_query_order import RowsQueryOrder
        from ..models.rows_query_request import RowsQueryRequest

        d = dict(src_dict)
        source = RowsQueryRequest.from_dict(d.pop("source"))

        group_by = cast(list[str], d.pop("group_by", UNSET))

        _aggregations = d.pop("aggregations", UNSET)
        aggregations: list[RowsAggregateSpec] | Unset = UNSET
        if _aggregations is not UNSET:
            aggregations = []
            for aggregations_item_data in _aggregations:
                aggregations_item = RowsAggregateSpec.from_dict(aggregations_item_data)

                aggregations.append(aggregations_item)

        _having = d.pop("having", UNSET)
        having: RowsAggregateHaving | Unset
        if isinstance(_having, Unset):
            having = UNSET
        else:
            having = RowsAggregateHaving.from_dict(_having)

        _order_by = d.pop("order_by", UNSET)
        order_by: list[RowsQueryOrder] | Unset = UNSET
        if _order_by is not UNSET:
            order_by = []
            for order_by_item_data in _order_by:
                order_by_item = RowsQueryOrder.from_dict(order_by_item_data)

                order_by.append(order_by_item)

        limit = d.pop("limit", UNSET)

        offset = d.pop("offset", UNSET)

        rows_aggregate_request = cls(
            source=source,
            group_by=group_by,
            aggregations=aggregations,
            having=having,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

        rows_aggregate_request.additional_properties = d
        return rows_aggregate_request

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
