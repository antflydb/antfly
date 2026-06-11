from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_query_request import RowsQueryRequest


T = TypeVar("T", bound="RowsCte")


@_attrs_define
class RowsCte:
    """Ordered named row-query subplan. Later CTEs and final plan stages can reference earlier names through `source_cte`.
    `max_rows` and `max_bytes` are optional materialization bounds; execution fails closed when the CTE would produce
    more rows or serialized row bytes than declared.

        Attributes:
            name (str):
            query (RowsQueryRequest): Typed relational row-query plan. Predicate and expression arrays carry
                Antfly row-expression AST nodes; SQL syntax is adapter sugar over this
                native request shape.
            max_rows (int | Unset):
            max_bytes (int | Unset):
    """

    name: str
    query: RowsQueryRequest
    max_rows: int | Unset = UNSET
    max_bytes: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        query = self.query.to_dict()

        max_rows = self.max_rows

        max_bytes = self.max_bytes

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "query": query,
            }
        )
        if max_rows is not UNSET:
            field_dict["max_rows"] = max_rows
        if max_bytes is not UNSET:
            field_dict["max_bytes"] = max_bytes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_query_request import RowsQueryRequest

        d = dict(src_dict)
        name = d.pop("name")

        query = RowsQueryRequest.from_dict(d.pop("query"))

        max_rows = d.pop("max_rows", UNSET)

        max_bytes = d.pop("max_bytes", UNSET)

        rows_cte = cls(
            name=name,
            query=query,
            max_rows=max_rows,
            max_bytes=max_bytes,
        )

        return rows_cte
