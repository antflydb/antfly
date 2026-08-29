from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.index_readiness_state import IndexReadinessState
from ..types import UNSET, Unset

T = TypeVar("T", bound="IndexReadinessStatus")


@_attrs_define
class IndexReadinessStatus:
    """
    Attributes:
        state (IndexReadinessState): Lifecycle state for the desired index incarnation. A failed desired repair may
            coexist with queryable=true when a separately proven serving incarnation remains available; clients must use the
            explicit milestone booleans.
        queryable (bool): Whether the published generation can safely answer queries.
        complete (bool): Whether the desired incarnation has complete coverage and publication according to its
            configured policies.
        pending_reasons (list[str]): Stable, machine-readable blockers. Empty when state is ready.
        incarnation (str | Unset): Opaque identity for the desired index incarnation. Clients may compare it for
            equality but must not interpret its contents.
        target_revision (int | Unset): Highest captured source/replay revision required by this readiness observation.
        published_revision (int | Unset): Highest revision published to the query-visible index represented by this
            observation.
    """

    state: IndexReadinessState
    queryable: bool
    complete: bool
    pending_reasons: list[str]
    incarnation: str | Unset = UNSET
    target_revision: int | Unset = UNSET
    published_revision: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        state = self.state.value

        queryable = self.queryable

        complete = self.complete

        pending_reasons = self.pending_reasons

        incarnation = self.incarnation

        target_revision = self.target_revision

        published_revision = self.published_revision

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
        if target_revision is not UNSET:
            field_dict["target_revision"] = target_revision
        if published_revision is not UNSET:
            field_dict["published_revision"] = published_revision

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        state = IndexReadinessState(d.pop("state"))

        queryable = d.pop("queryable")

        complete = d.pop("complete")

        pending_reasons = cast(list[str], d.pop("pending_reasons"))

        incarnation = d.pop("incarnation", UNSET)

        target_revision = d.pop("target_revision", UNSET)

        published_revision = d.pop("published_revision", UNSET)

        index_readiness_status = cls(
            state=state,
            queryable=queryable,
            complete=complete,
            pending_reasons=pending_reasons,
            incarnation=incarnation,
            target_revision=target_revision,
            published_revision=published_revision,
        )

        return index_readiness_status
