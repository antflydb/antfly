from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_query_order import RowsQueryOrder
    from ..models.rows_query_request import RowsQueryRequest
    from ..models.rows_window_spec import RowsWindowSpec


T = TypeVar("T", bound="RowsWindowRequest")


@_attrs_define
class RowsWindowRequest:
    """
    Attributes:
        source (RowsQueryRequest): Typed relational row-query plan. Predicate and expression arrays carry
            Antfly row-expression AST nodes; SQL syntax is adapter sugar over this
            native request shape.
        windows (list[RowsWindowSpec]):
        select (list[str] | Unset):
        order_by (list[RowsQueryOrder] | Unset):
        limit (int | Unset):
        offset (int | Unset):
    """

    source: RowsQueryRequest
    windows: list[RowsWindowSpec]
    select: list[str] | Unset = UNSET
    order_by: list[RowsQueryOrder] | Unset = UNSET
    limit: int | Unset = UNSET
    offset: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        source = self.source.to_dict()

        windows = []
        for windows_item_data in self.windows:
            windows_item = windows_item_data.to_dict()
            windows.append(windows_item)

        select: list[str] | Unset = UNSET
        if not isinstance(self.select, Unset):
            select = self.select

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
                "windows": windows,
            }
        )
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
        from ..models.rows_query_order import RowsQueryOrder
        from ..models.rows_query_request import RowsQueryRequest
        from ..models.rows_window_spec import RowsWindowSpec

        d = dict(src_dict)
        source = RowsQueryRequest.from_dict(d.pop("source"))

        windows = []
        _windows = d.pop("windows")
        for windows_item_data in _windows:
            windows_item = RowsWindowSpec.from_dict(windows_item_data)

            windows.append(windows_item)

        select = cast(list[str], d.pop("select", UNSET))

        _order_by = d.pop("order_by", UNSET)
        order_by: list[RowsQueryOrder] | Unset = UNSET
        if _order_by is not UNSET:
            order_by = []
            for order_by_item_data in _order_by:
                order_by_item = RowsQueryOrder.from_dict(order_by_item_data)

                order_by.append(order_by_item)

        limit = d.pop("limit", UNSET)

        offset = d.pop("offset", UNSET)

        rows_window_request = cls(
            source=source,
            windows=windows,
            select=select,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )

        rows_window_request.additional_properties = d
        return rows_window_request

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
