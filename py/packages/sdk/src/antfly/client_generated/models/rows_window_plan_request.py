from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_cte import RowsCte
    from ..models.rows_window_request import RowsWindowRequest


T = TypeVar("T", bound="RowsWindowPlanRequest")


@_attrs_define
class RowsWindowPlanRequest:
    """Typed row-window plan envelope. Accepts exactly `window` plus optional ordered `ctes` and declared `ranges`.

    Attributes:
        window (RowsWindowRequest):
        ctes (list[RowsCte] | Unset):
        ranges (list[Any] | Unset):
    """

    window: RowsWindowRequest
    ctes: list[RowsCte] | Unset = UNSET
    ranges: list[Any] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        window = self.window.to_dict()

        ctes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.ctes, Unset):
            ctes = []
            for ctes_item_data in self.ctes:
                ctes_item = ctes_item_data.to_dict()
                ctes.append(ctes_item)

        ranges: list[Any] | Unset = UNSET
        if not isinstance(self.ranges, Unset):
            ranges = []
            for ranges_item_data in self.ranges:
                ranges_item: Any
                ranges_item = ranges_item_data
                ranges.append(ranges_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "window": window,
            }
        )
        if ctes is not UNSET:
            field_dict["ctes"] = ctes
        if ranges is not UNSET:
            field_dict["ranges"] = ranges

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_cte import RowsCte
        from ..models.rows_window_request import RowsWindowRequest

        d = dict(src_dict)
        window = RowsWindowRequest.from_dict(d.pop("window"))

        _ctes = d.pop("ctes", UNSET)
        ctes: list[RowsCte] | Unset = UNSET
        if _ctes is not UNSET:
            ctes = []
            for ctes_item_data in _ctes:
                ctes_item = RowsCte.from_dict(ctes_item_data)

                ctes.append(ctes_item)

        _ranges = d.pop("ranges", UNSET)
        ranges: list[Any] | Unset = UNSET
        if _ranges is not UNSET:
            ranges = []
            for ranges_item_data in _ranges:

                def _parse_ranges_item(data: object) -> Any:
                    return cast(Any, data)

                ranges_item = _parse_ranges_item(ranges_item_data)

                ranges.append(ranges_item)

        rows_window_plan_request = cls(
            window=window,
            ctes=ctes,
            ranges=ranges,
        )

        return rows_window_plan_request
