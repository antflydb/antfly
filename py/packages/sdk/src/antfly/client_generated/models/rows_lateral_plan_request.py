from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_cte import RowsCte
    from ..models.rows_lateral_request import RowsLateralRequest


T = TypeVar("T", bound="RowsLateralPlanRequest")


@_attrs_define
class RowsLateralPlanRequest:
    """Typed row-lateral plan envelope. Accepts exactly `lateral` plus optional ordered `ctes`.

    Attributes:
        lateral (RowsLateralRequest): Typed bounded lateral plan. The right side must include a limit and can read an
            ordered CTE through `source_cte`.
        ctes (list[RowsCte] | Unset):
    """

    lateral: RowsLateralRequest
    ctes: list[RowsCte] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        lateral = self.lateral.to_dict()

        ctes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.ctes, Unset):
            ctes = []
            for ctes_item_data in self.ctes:
                ctes_item = ctes_item_data.to_dict()
                ctes.append(ctes_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "lateral": lateral,
            }
        )
        if ctes is not UNSET:
            field_dict["ctes"] = ctes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_cte import RowsCte
        from ..models.rows_lateral_request import RowsLateralRequest

        d = dict(src_dict)
        lateral = RowsLateralRequest.from_dict(d.pop("lateral"))

        _ctes = d.pop("ctes", UNSET)
        ctes: list[RowsCte] | Unset = UNSET
        if _ctes is not UNSET:
            ctes = []
            for ctes_item_data in _ctes:
                ctes_item = RowsCte.from_dict(ctes_item_data)

                ctes.append(ctes_item)

        rows_lateral_plan_request = cls(
            lateral=lateral,
            ctes=ctes,
        )

        return rows_lateral_plan_request
