from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.enrichment_context_query_filter_query import EnrichmentContextQueryFilterQuery
    from ..models.enrichment_context_query_order_by import EnrichmentContextQueryOrderBy


T = TypeVar("T", bound="EnrichmentContextQuery")


@_attrs_define
class EnrichmentContextQuery:
    """
    Attributes:
        filter_query (EnrichmentContextQueryFilterQuery): `{"conjuncts":[...]}` (or one bare conjunct) of:
            `term`/`terms` (equality, indexed subscriptions; at most one bound `terms` set), `range` (`gt`/`gte`/`lt`/`lte`,
            numbers or strings such as ISO dates), `prefix`, `match` (filter-mode full-text with `operator` and|or,
            evaluated with the pinned `antfly.context.simple.v1` analyzer: lowercase ASCII words), and `similar` (`{vector,
            threshold, metric: cosine|dot, dims, model}` over a vector stored in a document field; `model` names the
            embedding space and is part of the declaration). Values are literals or typed bindings
            `{"$source":"/json/pointer"}` from the source document. Disjunctions, scored full-text, fuzzy/regex, and
            nearest-k are rejected for live tracking.
        table (str | Unset): Table queried; omitted means the enrichment's own table.
        order_by (EnrichmentContextQueryOrderBy | Unset): Top-N by a field (ties by key); `max_documents` is N.
            Invalidation tracks the current boundary.
    """

    filter_query: EnrichmentContextQueryFilterQuery
    table: str | Unset = UNSET
    order_by: EnrichmentContextQueryOrderBy | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        filter_query = self.filter_query.to_dict()

        table = self.table

        order_by: dict[str, Any] | Unset = UNSET
        if not isinstance(self.order_by, Unset):
            order_by = self.order_by.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "filter_query": filter_query,
            }
        )
        if table is not UNSET:
            field_dict["table"] = table
        if order_by is not UNSET:
            field_dict["order_by"] = order_by

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.enrichment_context_query_filter_query import EnrichmentContextQueryFilterQuery
        from ..models.enrichment_context_query_order_by import EnrichmentContextQueryOrderBy

        if set(src_dict) - {
            "filter_query",
            "table",
            "order_by",
        }:
            raise ValueError("Unexpected properties for EnrichmentContextQuery")
        d = dict(src_dict)
        filter_query = EnrichmentContextQueryFilterQuery.from_dict(d.pop("filter_query"))

        table = d.pop("table", UNSET)

        _order_by = d.pop("order_by", UNSET)
        order_by: EnrichmentContextQueryOrderBy | Unset
        if isinstance(_order_by, Unset):
            order_by = UNSET
        else:
            order_by = EnrichmentContextQueryOrderBy.from_dict(_order_by)

        enrichment_context_query = cls(
            filter_query=filter_query,
            table=table,
            order_by=order_by,
        )

        return enrichment_context_query
