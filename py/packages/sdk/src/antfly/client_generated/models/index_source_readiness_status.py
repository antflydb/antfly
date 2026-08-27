from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.index_source_readiness_status_state import IndexSourceReadinessStatusState

T = TypeVar("T", bound="IndexSourceReadinessStatus")


@_attrs_define
class IndexSourceReadinessStatus:
    """
    Attributes:
        artifact (str): Configured artifact stream identity.
        state (IndexSourceReadinessStatusState):
        complete (bool): Whether this source is published through its captured target revision on every expected shard.
        pending_reasons (list[str]): Stable, machine-readable blockers for this source. Empty when state is ready.
    """

    artifact: str
    state: IndexSourceReadinessStatusState
    complete: bool
    pending_reasons: list[str]

    def to_dict(self) -> dict[str, Any]:
        artifact = self.artifact

        state = self.state.value

        complete = self.complete

        pending_reasons = self.pending_reasons

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "artifact": artifact,
                "state": state,
                "complete": complete,
                "pending_reasons": pending_reasons,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        artifact = d.pop("artifact")

        state = IndexSourceReadinessStatusState(d.pop("state"))

        complete = d.pop("complete")

        pending_reasons = cast(list[str], d.pop("pending_reasons"))

        index_source_readiness_status = cls(
            artifact=artifact,
            state=state,
            complete=complete,
            pending_reasons=pending_reasons,
        )

        return index_source_readiness_status
