from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.index_readiness_reason import IndexReadinessReason
from ..models.index_readiness_state import IndexReadinessState
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.index_source_readiness_status import IndexSourceReadinessStatus


T = TypeVar("T", bound="IndexReadinessStatus")


@_attrs_define
class IndexReadinessStatus:
    """
    Attributes:
        state (IndexReadinessState): Authoritative query-readiness and completeness state for the desired index
            incarnation.
        queryable (bool): Whether the published generation can safely answer queries.
        complete (bool): Whether the desired incarnation has complete coverage and publication according to its
            configured policies.
        pending_reasons (list[IndexReadinessReason]): Stable, machine-readable blockers or failure reasons. Empty when
            state is ready.
        incarnation (str | Unset): Opaque identity for the desired index incarnation. Clients may compare it for
            equality but must not interpret its contents.
        sources (list[IndexSourceReadinessStatus] | Unset): Operational readiness for each configured artifact stream.
            Present only for artifact-backed indexes, in configuration order.
    """

    state: IndexReadinessState
    queryable: bool
    complete: bool
    pending_reasons: list[IndexReadinessReason]
    incarnation: str | Unset = UNSET
    sources: list[IndexSourceReadinessStatus] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        state = self.state.value

        queryable = self.queryable

        complete = self.complete

        pending_reasons = []
        for pending_reasons_item_data in self.pending_reasons:
            pending_reasons_item = pending_reasons_item_data.value
            pending_reasons.append(pending_reasons_item)

        incarnation = self.incarnation

        sources: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.sources, Unset):
            sources = []
            for sources_item_data in self.sources:
                sources_item = sources_item_data.to_dict()
                sources.append(sources_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "state": state,
                "queryable": queryable,
                "complete": complete,
                "pending_reasons": pending_reasons,
            }
        )
        if incarnation is not UNSET:
            field_dict["incarnation"] = incarnation
        if sources is not UNSET:
            field_dict["sources"] = sources

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.index_source_readiness_status import IndexSourceReadinessStatus

        d = dict(src_dict)
        state = IndexReadinessState(d.pop("state"))

        queryable = d.pop("queryable")

        complete = d.pop("complete")

        pending_reasons = []
        _pending_reasons = d.pop("pending_reasons")
        for pending_reasons_item_data in _pending_reasons:
            pending_reasons_item = IndexReadinessReason(pending_reasons_item_data)

            pending_reasons.append(pending_reasons_item)

        incarnation = d.pop("incarnation", UNSET)

        _sources = d.pop("sources", UNSET)
        sources: list[IndexSourceReadinessStatus] | Unset = UNSET
        if _sources is not UNSET:
            sources = []
            for sources_item_data in _sources:
                sources_item = IndexSourceReadinessStatus.from_dict(sources_item_data)

                sources.append(sources_item)

        index_readiness_status = cls(
            state=state,
            queryable=queryable,
            complete=complete,
            pending_reasons=pending_reasons,
            incarnation=incarnation,
            sources=sources,
        )

        return index_readiness_status
