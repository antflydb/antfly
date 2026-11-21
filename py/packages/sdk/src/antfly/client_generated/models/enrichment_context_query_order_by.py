from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.enrichment_context_query_order_by_order import EnrichmentContextQueryOrderByOrder
from ..types import UNSET, Unset

T = TypeVar("T", bound="EnrichmentContextQueryOrderBy")


@_attrs_define
class EnrichmentContextQueryOrderBy:
    """Top-N by a field (ties by key); `max_documents` is N. Invalidation tracks the current boundary.

    Attributes:
        field (str):
        order (EnrichmentContextQueryOrderByOrder | Unset):  Default: EnrichmentContextQueryOrderByOrder.ASC.
    """

    field: str
    order: EnrichmentContextQueryOrderByOrder | Unset = EnrichmentContextQueryOrderByOrder.ASC

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        order: str | Unset = UNSET
        if not isinstance(self.order, Unset):
            order = self.order.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
            }
        )
        if order is not UNSET:
            field_dict["order"] = order

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        _order = d.pop("order", UNSET)
        order: EnrichmentContextQueryOrderByOrder | Unset
        if isinstance(_order, Unset):
            order = UNSET
        else:
            order = EnrichmentContextQueryOrderByOrder(_order)

        enrichment_context_query_order_by = cls(
            field=field,
            order=order,
        )

        return enrichment_context_query_order_by
