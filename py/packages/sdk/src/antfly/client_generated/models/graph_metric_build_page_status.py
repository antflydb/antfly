from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_metric_build_page_status_range_kind import GraphMetricBuildPageStatusRangeKind
from ..models.graph_metric_build_page_status_state import GraphMetricBuildPageStatusState
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphMetricBuildPageStatus")


@_attrs_define
class GraphMetricBuildPageStatus:
    """
    Attributes:
        phase (str):
        iteration (int):
        page_id (int):
        state (GraphMetricBuildPageStatusState):
        range_kind (GraphMetricBuildPageStatusRangeKind):
        worker_id (str | Unset): Worker id that owns or last failed this page.
        lease_expires_at_ms (int | Unset): Unix epoch milliseconds when the page lease expires, or 0 when not leased.
        attempt (int | Unset): Current attempt number for this page.
        cursor (str | Unset): Opaque resumable cursor for this page.
        completed_units (int | Unset): Completed work units for this page.
        total_units (int | Unset): Estimated total work units for this page.
        last_error (str | Unset): Last page-level error.
    """

    phase: str
    iteration: int
    page_id: int
    state: GraphMetricBuildPageStatusState
    range_kind: GraphMetricBuildPageStatusRangeKind
    worker_id: str | Unset = UNSET
    lease_expires_at_ms: int | Unset = UNSET
    attempt: int | Unset = UNSET
    cursor: str | Unset = UNSET
    completed_units: int | Unset = UNSET
    total_units: int | Unset = UNSET
    last_error: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        phase = self.phase

        iteration = self.iteration

        page_id = self.page_id

        state = self.state.value

        range_kind = self.range_kind.value

        worker_id = self.worker_id

        lease_expires_at_ms = self.lease_expires_at_ms

        attempt = self.attempt

        cursor = self.cursor

        completed_units = self.completed_units

        total_units = self.total_units

        last_error = self.last_error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "phase": phase,
                "iteration": iteration,
                "page_id": page_id,
                "state": state,
                "range_kind": range_kind,
            }
        )
        if worker_id is not UNSET:
            field_dict["worker_id"] = worker_id
        if lease_expires_at_ms is not UNSET:
            field_dict["lease_expires_at_ms"] = lease_expires_at_ms
        if attempt is not UNSET:
            field_dict["attempt"] = attempt
        if cursor is not UNSET:
            field_dict["cursor"] = cursor
        if completed_units is not UNSET:
            field_dict["completed_units"] = completed_units
        if total_units is not UNSET:
            field_dict["total_units"] = total_units
        if last_error is not UNSET:
            field_dict["last_error"] = last_error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        phase = d.pop("phase")

        iteration = d.pop("iteration")

        page_id = d.pop("page_id")

        state = GraphMetricBuildPageStatusState(d.pop("state"))

        range_kind = GraphMetricBuildPageStatusRangeKind(d.pop("range_kind"))

        worker_id = d.pop("worker_id", UNSET)

        lease_expires_at_ms = d.pop("lease_expires_at_ms", UNSET)

        attempt = d.pop("attempt", UNSET)

        cursor = d.pop("cursor", UNSET)

        completed_units = d.pop("completed_units", UNSET)

        total_units = d.pop("total_units", UNSET)

        last_error = d.pop("last_error", UNSET)

        graph_metric_build_page_status = cls(
            phase=phase,
            iteration=iteration,
            page_id=page_id,
            state=state,
            range_kind=range_kind,
            worker_id=worker_id,
            lease_expires_at_ms=lease_expires_at_ms,
            attempt=attempt,
            cursor=cursor,
            completed_units=completed_units,
            total_units=total_units,
            last_error=last_error,
        )

        graph_metric_build_page_status.additional_properties = d
        return graph_metric_build_page_status

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
