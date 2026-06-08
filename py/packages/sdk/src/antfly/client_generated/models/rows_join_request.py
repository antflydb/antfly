from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.rows_join_request_join_type import RowsJoinRequestJoinType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_join_on import RowsJoinOn
    from ..models.rows_join_projection import RowsJoinProjection
    from ..models.rows_query_order import RowsQueryOrder
    from ..models.rows_query_request import RowsQueryRequest


T = TypeVar("T", bound="RowsJoinRequest")


@_attrs_define
class RowsJoinRequest:
    """Typed equality join plan. Each side is a full row-query request and can read an ordered CTE through `source_cte`.

    Attributes:
        left (RowsQueryRequest): Typed relational row-query plan. Predicate and expression arrays carry
            Antfly row-expression AST nodes; SQL syntax is adapter sugar over this
            native request shape.
        right (RowsQueryRequest): Typed relational row-query plan. Predicate and expression arrays carry
            Antfly row-expression AST nodes; SQL syntax is adapter sugar over this
            native request shape.
        on (list[RowsJoinOn]):
        join_type (RowsJoinRequestJoinType | Unset):
        select (list[RowsJoinProjection] | Unset):
        order_by (list[RowsQueryOrder] | Unset):
        limit (int | Unset):
        offset (int | Unset):
    """

    left: RowsQueryRequest
    right: RowsQueryRequest
    on: list[RowsJoinOn]
    join_type: RowsJoinRequestJoinType | Unset = UNSET
    select: list[RowsJoinProjection] | Unset = UNSET
    order_by: list[RowsQueryOrder] | Unset = UNSET
    limit: int | Unset = UNSET
    offset: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        left = self.left.to_dict()

        right = self.right.to_dict()

        on = []
        for on_item_data in self.on:
            on_item = on_item_data.to_dict()
            on.append(on_item)

        join_type: str | Unset = UNSET
        if not isinstance(self.join_type, Unset):
            join_type = self.join_type.value

        select: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.select, Unset):
            select = []
            for select_item_data in self.select:
                select_item = select_item_data.to_dict()
                select.append(select_item)

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
                "left": left,
                "right": right,
                "on": on,
            }
        )
        if join_type is not UNSET:
            field_dict["join_type"] = join_type
        if select is not UNSET:
            field_dict["select"] = select
        if order_by is not UNSET:
            field_dict["order_by"] = order_by
        if limit is not UNSET:
            field_dict["limit"] = limit
        if offset is not UNSET:
            field_dict["offset"] = offset

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_join_on import RowsJoinOn
        from ..models.rows_join_projection import RowsJoinProjection
        from ..models.rows_query_order import RowsQueryOrder
        from ..models.rows_query_request import RowsQueryRequest

        d = dict(src_dict)
        left = RowsQueryRequest.from_dict(d.pop("left"))

        right = RowsQueryRequest.from_dict(d.pop("right"))

        on = []
        _on = d.pop("on")
        for on_item_data in _on:
            on_item = RowsJoinOn.from_dict(on_item_data)

            on.append(on_item)

        _join_type = d.pop("join_type", UNSET)
        join_type: RowsJoinRequestJoinType | Unset
        if isinstance(_join_type, Unset):
            join_type = UNSET
        else:
            join_type = RowsJoinRequestJoinType(_join_type)

        _select = d.pop("select", UNSET)
        select: list[RowsJoinProjection] | Unset = UNSET
        if _select is not UNSET:
            select = []
            for select_item_data in _select:
                select_item = RowsJoinProjection.from_dict(select_item_data)

                select.append(select_item)

        _order_by = d.pop("order_by", UNSET)
        order_by: list[RowsQueryOrder] | Unset = UNSET
        if _order_by is not UNSET:
            order_by = []
            for order_by_item_data in _order_by:
                order_by_item = RowsQueryOrder.from_dict(order_by_item_data)

                order_by.append(order_by_item)

        limit = d.pop("limit", UNSET)

        offset = d.pop("offset", UNSET)

        rows_join_request = cls(
            left=left,
            right=right,
            on=on,
            join_type=join_type,
            select=select,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

        rows_join_request.additional_properties = d
        return rows_join_request

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
