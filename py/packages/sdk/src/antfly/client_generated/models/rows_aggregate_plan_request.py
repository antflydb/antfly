from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_aggregate_request import RowsAggregateRequest
    from ..models.rows_cte import RowsCte


T = TypeVar("T", bound="RowsAggregatePlanRequest")


@_attrs_define
class RowsAggregatePlanRequest:
    """Typed row-aggregate plan envelope. Accepts exactly `aggregate` plus optional ordered `ctes`.

    Attributes:
        aggregate (RowsAggregateRequest):
        ctes (list[RowsCte] | Unset):
    """

    aggregate: RowsAggregateRequest
    ctes: list[RowsCte] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        aggregate = self.aggregate.to_dict()

        ctes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.ctes, Unset):
            ctes = []
            for ctes_item_data in self.ctes:
                ctes_item = ctes_item_data.to_dict()
                ctes.append(ctes_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "aggregate": aggregate,
            }
        )
        if ctes is not UNSET:
            field_dict["ctes"] = ctes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_aggregate_request import RowsAggregateRequest
        from ..models.rows_cte import RowsCte

        d = dict(src_dict)
        aggregate = RowsAggregateRequest.from_dict(d.pop("aggregate"))

        _ctes = d.pop("ctes", UNSET)
        ctes: list[RowsCte] | Unset = UNSET
        if _ctes is not UNSET:
            ctes = []
            for ctes_item_data in _ctes:
                ctes_item = RowsCte.from_dict(ctes_item_data)

                ctes.append(ctes_item)

        rows_aggregate_plan_request = cls(
            aggregate=aggregate,
            ctes=ctes,
        )

        return rows_aggregate_plan_request
