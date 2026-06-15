from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_cte import RowsCte
    from ..models.rows_join_request import RowsJoinRequest


T = TypeVar("T", bound="RowsJoinPlanRequest")


@_attrs_define
class RowsJoinPlanRequest:
    """Typed row-join plan envelope. Accepts exactly `join` plus optional ordered `ctes`, optional left/right table names,
    and paired `left_ranges` and `right_ranges`.

        Attributes:
            join (RowsJoinRequest): Typed equality join plan. Each side is a full row-query request and can read an ordered
                CTE through `source_cte`.
            ctes (list[RowsCte] | Unset):
            left_table (str | Unset): Optional source table for the left side. Omitted or empty uses the endpoint table.
            right_table (str | Unset): Optional source table for the right side. Omitted or empty uses the endpoint table.
            left_ranges (list[Any] | Unset):
            right_ranges (list[Any] | Unset):
    """

    join: RowsJoinRequest
    ctes: list[RowsCte] | Unset = UNSET
    left_table: str | Unset = UNSET
    right_table: str | Unset = UNSET
    left_ranges: list[Any] | Unset = UNSET
    right_ranges: list[Any] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        join = self.join.to_dict()

        ctes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.ctes, Unset):
            ctes = []
            for ctes_item_data in self.ctes:
                ctes_item = ctes_item_data.to_dict()
                ctes.append(ctes_item)

        left_table = self.left_table

        right_table = self.right_table

        left_ranges: list[Any] | Unset = UNSET
        if not isinstance(self.left_ranges, Unset):
            left_ranges = []
            for left_ranges_item_data in self.left_ranges:
                left_ranges_item: Any
                left_ranges_item = left_ranges_item_data
                left_ranges.append(left_ranges_item)

        right_ranges: list[Any] | Unset = UNSET
        if not isinstance(self.right_ranges, Unset):
            right_ranges = []
            for right_ranges_item_data in self.right_ranges:
                right_ranges_item: Any
                right_ranges_item = right_ranges_item_data
                right_ranges.append(right_ranges_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "join": join,
            }
        )
        if ctes is not UNSET:
            field_dict["ctes"] = ctes
        if left_table is not UNSET:
            field_dict["left_table"] = left_table
        if right_table is not UNSET:
            field_dict["right_table"] = right_table
        if left_ranges is not UNSET:
            field_dict["left_ranges"] = left_ranges
        if right_ranges is not UNSET:
            field_dict["right_ranges"] = right_ranges

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_cte import RowsCte
        from ..models.rows_join_request import RowsJoinRequest

        d = dict(src_dict)
        join = RowsJoinRequest.from_dict(d.pop("join"))

        _ctes = d.pop("ctes", UNSET)
        ctes: list[RowsCte] | Unset = UNSET
        if _ctes is not UNSET:
            ctes = []
            for ctes_item_data in _ctes:
                ctes_item = RowsCte.from_dict(ctes_item_data)

                ctes.append(ctes_item)

        left_table = d.pop("left_table", UNSET)

        right_table = d.pop("right_table", UNSET)

        _left_ranges = d.pop("left_ranges", UNSET)
        left_ranges: list[Any] | Unset = UNSET
        if _left_ranges is not UNSET:
            left_ranges = []
            for left_ranges_item_data in _left_ranges:

                def _parse_left_ranges_item(data: object) -> Any:
                    return cast(Any, data)

                left_ranges_item = _parse_left_ranges_item(left_ranges_item_data)

                left_ranges.append(left_ranges_item)

        _right_ranges = d.pop("right_ranges", UNSET)
        right_ranges: list[Any] | Unset = UNSET
        if _right_ranges is not UNSET:
            right_ranges = []
            for right_ranges_item_data in _right_ranges:

                def _parse_right_ranges_item(data: object) -> Any:
                    return cast(Any, data)

                right_ranges_item = _parse_right_ranges_item(right_ranges_item_data)

                right_ranges.append(right_ranges_item)

        rows_join_plan_request = cls(
            join=join,
            ctes=ctes,
            left_table=left_table,
            right_table=right_table,
            left_ranges=left_ranges,
            right_ranges=right_ranges,
        )

        return rows_join_plan_request
