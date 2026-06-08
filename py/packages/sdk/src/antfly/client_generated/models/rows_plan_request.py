from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_aggregate_request import RowsAggregateRequest
    from ..models.rows_cte import RowsCte
    from ..models.rows_join_request import RowsJoinRequest
    from ..models.rows_lateral_request import RowsLateralRequest
    from ..models.rows_query_request import RowsQueryRequest
    from ..models.rows_window_request import RowsWindowRequest


T = TypeVar("T", bound="RowsPlanRequest")


@_attrs_define
class RowsPlanRequest:
    """Generic typed relational row plan envelope. Public operation endpoints
    use the operation-specific envelope schemas below and accept exactly one
    top-level operation field plus optional ordered `ctes`.

        Attributes:
            ctes (list[RowsCte] | Unset):
            query (RowsQueryRequest | Unset): Typed relational row-query plan. Predicate and expression arrays carry
                Antfly row-expression AST nodes; SQL syntax is adapter sugar over this
                native request shape.
            aggregate (RowsAggregateRequest | Unset):
            window (RowsWindowRequest | Unset):
            join (RowsJoinRequest | Unset): Typed equality join plan. Each side is a full row-query request and can read an
                ordered CTE through `source_cte`.
            lateral (RowsLateralRequest | Unset): Typed bounded lateral plan. The right side must include a limit and can
                read an ordered CTE through `source_cte`.
    """

    ctes: list[RowsCte] | Unset = UNSET
    query: RowsQueryRequest | Unset = UNSET
    aggregate: RowsAggregateRequest | Unset = UNSET
    window: RowsWindowRequest | Unset = UNSET
    join: RowsJoinRequest | Unset = UNSET
    lateral: RowsLateralRequest | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        ctes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.ctes, Unset):
            ctes = []
            for ctes_item_data in self.ctes:
                ctes_item = ctes_item_data.to_dict()
                ctes.append(ctes_item)

        query: dict[str, Any] | Unset = UNSET
        if not isinstance(self.query, Unset):
            query = self.query.to_dict()

        aggregate: dict[str, Any] | Unset = UNSET
        if not isinstance(self.aggregate, Unset):
            aggregate = self.aggregate.to_dict()

        window: dict[str, Any] | Unset = UNSET
        if not isinstance(self.window, Unset):
            window = self.window.to_dict()

        join: dict[str, Any] | Unset = UNSET
        if not isinstance(self.join, Unset):
            join = self.join.to_dict()

        lateral: dict[str, Any] | Unset = UNSET
        if not isinstance(self.lateral, Unset):
            lateral = self.lateral.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if ctes is not UNSET:
            field_dict["ctes"] = ctes
        if query is not UNSET:
            field_dict["query"] = query
        if aggregate is not UNSET:
            field_dict["aggregate"] = aggregate
        if window is not UNSET:
            field_dict["window"] = window
        if join is not UNSET:
            field_dict["join"] = join
        if lateral is not UNSET:
            field_dict["lateral"] = lateral

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_aggregate_request import RowsAggregateRequest
        from ..models.rows_cte import RowsCte
        from ..models.rows_join_request import RowsJoinRequest
        from ..models.rows_lateral_request import RowsLateralRequest
        from ..models.rows_query_request import RowsQueryRequest
        from ..models.rows_window_request import RowsWindowRequest

        d = dict(src_dict)
        _ctes = d.pop("ctes", UNSET)
        ctes: list[RowsCte] | Unset = UNSET
        if _ctes is not UNSET:
            ctes = []
            for ctes_item_data in _ctes:
                ctes_item = RowsCte.from_dict(ctes_item_data)

                ctes.append(ctes_item)

        _query = d.pop("query", UNSET)
        query: RowsQueryRequest | Unset
        if isinstance(_query, Unset):
            query = UNSET
        else:
            query = RowsQueryRequest.from_dict(_query)

        _aggregate = d.pop("aggregate", UNSET)
        aggregate: RowsAggregateRequest | Unset
        if isinstance(_aggregate, Unset):
            aggregate = UNSET
        else:
            aggregate = RowsAggregateRequest.from_dict(_aggregate)

        _window = d.pop("window", UNSET)
        window: RowsWindowRequest | Unset
        if isinstance(_window, Unset):
            window = UNSET
        else:
            window = RowsWindowRequest.from_dict(_window)

        _join = d.pop("join", UNSET)
        join: RowsJoinRequest | Unset
        if isinstance(_join, Unset):
            join = UNSET
        else:
            join = RowsJoinRequest.from_dict(_join)

        _lateral = d.pop("lateral", UNSET)
        lateral: RowsLateralRequest | Unset
        if isinstance(_lateral, Unset):
            lateral = UNSET
        else:
            lateral = RowsLateralRequest.from_dict(_lateral)

        rows_plan_request = cls(
            ctes=ctes,
            query=query,
            aggregate=aggregate,
            window=window,
            join=join,
            lateral=lateral,
        )

        rows_plan_request.additional_properties = d
        return rows_plan_request

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
