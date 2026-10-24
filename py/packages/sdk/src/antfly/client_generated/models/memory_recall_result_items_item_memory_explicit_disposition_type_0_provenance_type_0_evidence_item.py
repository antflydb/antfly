from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_recall_result_items_item_memory_explicit_disposition_type_0_provenance_type_0_evidence_item_span_type_0 import (
        MemoryRecallResultItemsItemMemoryExplicitDispositionType0ProvenanceType0EvidenceItemSpanType0,
    )


T = TypeVar("T", bound="MemoryRecallResultItemsItemMemoryExplicitDispositionType0ProvenanceType0EvidenceItem")


@_attrs_define
class MemoryRecallResultItemsItemMemoryExplicitDispositionType0ProvenanceType0EvidenceItem:
    """
    Attributes:
        id (str):
        span (MemoryRecallResultItemsItemMemoryExplicitDispositionType0ProvenanceType0EvidenceItemSpanType0 | None |
            Unset):
    """

    id: str
    span: (
        MemoryRecallResultItemsItemMemoryExplicitDispositionType0ProvenanceType0EvidenceItemSpanType0 | None | Unset
    ) = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_recall_result_items_item_memory_explicit_disposition_type_0_provenance_type_0_evidence_item_span_type_0 import (
            MemoryRecallResultItemsItemMemoryExplicitDispositionType0ProvenanceType0EvidenceItemSpanType0,
        )

        id = self.id

        span: dict[str, Any] | None | Unset
        if isinstance(self.span, Unset):
            span = UNSET
        elif isinstance(
            self.span, MemoryRecallResultItemsItemMemoryExplicitDispositionType0ProvenanceType0EvidenceItemSpanType0
        ):
            span = self.span.to_dict()
        else:
            span = self.span

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
            }
        )
        if span is not UNSET:
            field_dict["span"] = span

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_recall_result_items_item_memory_explicit_disposition_type_0_provenance_type_0_evidence_item_span_type_0 import (
            MemoryRecallResultItemsItemMemoryExplicitDispositionType0ProvenanceType0EvidenceItemSpanType0,
        )

        d = dict(src_dict)
        id = d.pop("id")

        def _parse_span(
            data: object,
        ) -> (
            MemoryRecallResultItemsItemMemoryExplicitDispositionType0ProvenanceType0EvidenceItemSpanType0 | None | Unset
        ):
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                span_type_0 = MemoryRecallResultItemsItemMemoryExplicitDispositionType0ProvenanceType0EvidenceItemSpanType0.from_dict(
                    data
                )

                return span_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(
                MemoryRecallResultItemsItemMemoryExplicitDispositionType0ProvenanceType0EvidenceItemSpanType0
                | None
                | Unset,
                data,
            )

        span = _parse_span(d.pop("span", UNSET))

        memory_recall_result_items_item_memory_explicit_disposition_type_0_provenance_type_0_evidence_item = cls(
            id=id,
            span=span,
        )

        return memory_recall_result_items_item_memory_explicit_disposition_type_0_provenance_type_0_evidence_item
