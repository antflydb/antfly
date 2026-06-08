from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_cte import RowsCte
    from ..models.rows_join_request import RowsJoinRequest


T = TypeVar("T", bound="RowsJoinPlanRequest")


@_attrs_define
class RowsJoinPlanRequest:
    """Typed row-join plan envelope. Accepts exactly `join` plus optional ordered `ctes`.

    Attributes:
        join (RowsJoinRequest): Typed equality join plan. Each side is a full row-query request and can read an ordered
            CTE through `source_cte`.
        ctes (list[RowsCte] | Unset):
    """

    join: RowsJoinRequest
    ctes: list[RowsCte] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        join = self.join.to_dict()

        ctes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.ctes, Unset):
            ctes = []
            for ctes_item_data in self.ctes:
                ctes_item = ctes_item_data.to_dict()
                ctes.append(ctes_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "join": join,
            }
        )
        if ctes is not UNSET:
            field_dict["ctes"] = ctes

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

        rows_join_plan_request = cls(
            join=join,
            ctes=ctes,
        )

        return rows_join_plan_request
