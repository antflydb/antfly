from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.trace_event_status import TraceEventStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.trace_event_provenance import TraceEventProvenance


T = TypeVar("T", bound="TraceEvent")


@_attrs_define
class TraceEvent:
    """
    Attributes:
        trace_id (str):
        span_id (str):
        name (str):
        started_at (int):
        schema_version (int | Unset):  Default: 1.
        parent_span_id (None | str | Unset):
        ended_at (int | None | Unset):
        status (TraceEventStatus | Unset):  Default: TraceEventStatus.RUNNING.
        provenance (TraceEventProvenance | Unset):
    """

    trace_id: str
    span_id: str
    name: str
    started_at: int
    schema_version: int | Unset = 1
    parent_span_id: None | str | Unset = UNSET
    ended_at: int | None | Unset = UNSET
    status: TraceEventStatus | Unset = TraceEventStatus.RUNNING
    provenance: TraceEventProvenance | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        trace_id = self.trace_id

        span_id = self.span_id

        name = self.name

        started_at = self.started_at

        schema_version = self.schema_version

        parent_span_id: None | str | Unset
        if isinstance(self.parent_span_id, Unset):
            parent_span_id = UNSET
        else:
            parent_span_id = self.parent_span_id

        ended_at: int | None | Unset
        if isinstance(self.ended_at, Unset):
            ended_at = UNSET
        else:
            ended_at = self.ended_at

        status: str | Unset = UNSET
        if not isinstance(self.status, Unset):
            status = self.status.value

        provenance: dict[str, Any] | Unset = UNSET
        if not isinstance(self.provenance, Unset):
            provenance = self.provenance.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "trace_id": trace_id,
                "span_id": span_id,
                "name": name,
                "started_at": started_at,
            }
        )
        if schema_version is not UNSET:
            field_dict["schema_version"] = schema_version
        if parent_span_id is not UNSET:
            field_dict["parent_span_id"] = parent_span_id
        if ended_at is not UNSET:
            field_dict["ended_at"] = ended_at
        if status is not UNSET:
            field_dict["status"] = status
        if provenance is not UNSET:
            field_dict["provenance"] = provenance

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.trace_event_provenance import TraceEventProvenance

        d = dict(src_dict)
        trace_id = d.pop("trace_id")

        span_id = d.pop("span_id")

        name = d.pop("name")

        started_at = d.pop("started_at")

        schema_version = d.pop("schema_version", UNSET)

        def _parse_parent_span_id(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        parent_span_id = _parse_parent_span_id(d.pop("parent_span_id", UNSET))

        def _parse_ended_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        ended_at = _parse_ended_at(d.pop("ended_at", UNSET))

        _status = d.pop("status", UNSET)
        status: TraceEventStatus | Unset
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = TraceEventStatus(_status)

        _provenance = d.pop("provenance", UNSET)
        provenance: TraceEventProvenance | Unset
        if isinstance(_provenance, Unset):
            provenance = UNSET
        else:
            provenance = TraceEventProvenance.from_dict(_provenance)

        trace_event = cls(
            trace_id=trace_id,
            span_id=span_id,
            name=name,
            started_at=started_at,
            schema_version=schema_version,
            parent_span_id=parent_span_id,
            ended_at=ended_at,
            status=status,
            provenance=provenance,
        )

        return trace_event
