from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_cte import RowsCte
    from ..models.rows_query_request import RowsQueryRequest


T = TypeVar("T", bound="RowsQueryPlanRequest")


@_attrs_define
class RowsQueryPlanRequest:
    """Typed row-query plan envelope. Accepts exactly `query` plus optional ordered `ctes`.

    Attributes:
        query (RowsQueryRequest): Typed relational row-query plan. Predicate and expression arrays carry
            Antfly row-expression AST nodes; SQL syntax is adapter sugar over this
            native request shape.
        ctes (list[RowsCte] | Unset):
    """

    query: RowsQueryRequest
    ctes: list[RowsCte] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        query = self.query.to_dict()

        ctes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.ctes, Unset):
            ctes = []
            for ctes_item_data in self.ctes:
                ctes_item = ctes_item_data.to_dict()
                ctes.append(ctes_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "query": query,
            }
        )
        if ctes is not UNSET:
            field_dict["ctes"] = ctes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_cte import RowsCte
        from ..models.rows_query_request import RowsQueryRequest

        d = dict(src_dict)
        query = RowsQueryRequest.from_dict(d.pop("query"))

        _ctes = d.pop("ctes", UNSET)
        ctes: list[RowsCte] | Unset = UNSET
        if _ctes is not UNSET:
            ctes = []
            for ctes_item_data in _ctes:
                ctes_item = RowsCte.from_dict(ctes_item_data)

                ctes.append(ctes_item)

        rows_query_plan_request = cls(
            query=query,
            ctes=ctes,
        )

        return rows_query_plan_request
