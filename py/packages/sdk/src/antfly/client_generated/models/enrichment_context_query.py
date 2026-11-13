from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.enrichment_context_query_filter_query import EnrichmentContextQueryFilterQuery


T = TypeVar("T", bound="EnrichmentContextQuery")


@_attrs_define
class EnrichmentContextQuery:
    """
    Attributes:
        filter_query (EnrichmentContextQueryFilterQuery): A `{"conjuncts":[...]}` of `term`/`terms` equality conjuncts
            (or one bare conjunct). Values are literals or typed bindings `{"$source":"/json/pointer"}` from the source
            document (never text interpolation); at most one bound `terms` set. Ranges, disjunctions, full-text, and
            semantic top-k are rejected at admission. Members are canonically ordered by key.
        table (str | Unset): Table queried; omitted means the enrichment's own table.
    """

    filter_query: EnrichmentContextQueryFilterQuery
    table: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        filter_query = self.filter_query.to_dict()

        table = self.table

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "filter_query": filter_query,
            }
        )
        if table is not UNSET:
            field_dict["table"] = table

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.enrichment_context_query_filter_query import EnrichmentContextQueryFilterQuery

        if set(src_dict) - {
            "filter_query",
            "table",
        }:
            raise ValueError("Unexpected properties for EnrichmentContextQuery")
        d = dict(src_dict)
        filter_query = EnrichmentContextQueryFilterQuery.from_dict(d.pop("filter_query"))

        table = d.pop("table", UNSET)

        enrichment_context_query = cls(
            filter_query=filter_query,
            table=table,
        )

        return enrichment_context_query
