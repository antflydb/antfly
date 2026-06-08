from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_cte import RowsCte
    from ..models.rows_window_request import RowsWindowRequest


T = TypeVar("T", bound="RowsWindowPlanRequest")


@_attrs_define
class RowsWindowPlanRequest:
    """Typed row-window plan envelope. Accepts exactly `window` plus optional ordered `ctes`.

    Attributes:
        window (RowsWindowRequest):
        ctes (list[RowsCte] | Unset):
    """

    window: RowsWindowRequest
    ctes: list[RowsCte] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        window = self.window.to_dict()

        ctes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.ctes, Unset):
            ctes = []
            for ctes_item_data in self.ctes:
                ctes_item = ctes_item_data.to_dict()
                ctes.append(ctes_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "window": window,
            }
        )
        if ctes is not UNSET:
            field_dict["ctes"] = ctes

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

        rows_window_plan_request = cls(
            window=window,
            ctes=ctes,
        )

        return rows_window_plan_request
