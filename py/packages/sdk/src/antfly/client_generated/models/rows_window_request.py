from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_query_order_expression import RowsQueryOrderExpression
    from ..models.rows_query_order_field import RowsQueryOrderField
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
        order_by (list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset):
        limit (int | Unset):
        offset (int | Unset):
    """

    source: RowsQueryRequest
    windows: list[RowsWindowSpec]
    select: list[str] | Unset = UNSET
    order_by: list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset = UNSET
    limit: int | Unset = UNSET
    offset: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.rows_query_order_field import RowsQueryOrderField

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
                order_by_item: dict[str, Any]
                if isinstance(order_by_item_data, RowsQueryOrderField):
                    order_by_item = order_by_item_data.to_dict()
                else:
                    order_by_item = order_by_item_data.to_dict()

                order_by.append(order_by_item)

        limit = self.limit

        offset = self.offset

        field_dict: dict[str, Any] = {}

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
        from ..models.rows_query_order_expression import RowsQueryOrderExpression
        from ..models.rows_query_order_field import RowsQueryOrderField
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
        order_by: list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset = UNSET
        if _order_by is not UNSET:
            order_by = []
            for order_by_item_data in _order_by:

                def _parse_order_by_item(data: object) -> RowsQueryOrderExpression | RowsQueryOrderField:
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_rows_query_order_type_0 = RowsQueryOrderField.from_dict(data)

                        return componentsschemas_rows_query_order_type_0
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_rows_query_order_type_1 = RowsQueryOrderExpression.from_dict(data)

                    return componentsschemas_rows_query_order_type_1

                order_by_item = _parse_order_by_item(order_by_item_data)

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

        return rows_window_request
