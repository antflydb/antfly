from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_observation_request_origin_type_0_agent_type_0 import (
        MemoryObservationRequestOriginType0AgentType0,
    )
    from ..models.memory_observation_request_origin_type_0_approver_type_0 import (
        MemoryObservationRequestOriginType0ApproverType0,
    )
    from ..models.memory_observation_request_origin_type_0_span_type_0 import (
        MemoryObservationRequestOriginType0SpanType0,
    )
    from ..models.memory_observation_request_origin_type_0_trace_type_0 import (
        MemoryObservationRequestOriginType0TraceType0,
    )


T = TypeVar("T", bound="MemoryObservationRequestOriginType0")


@_attrs_define
class MemoryObservationRequestOriginType0:
    """
    Attributes:
        agent (MemoryObservationRequestOriginType0AgentType0 | None | Unset):
        approver (MemoryObservationRequestOriginType0ApproverType0 | None | Unset):
        trace (MemoryObservationRequestOriginType0TraceType0 | None | Unset):
        span (MemoryObservationRequestOriginType0SpanType0 | None | Unset):
        workflow (None | str | Unset):
        workflow_version (None | str | Unset):
    """

    agent: MemoryObservationRequestOriginType0AgentType0 | None | Unset = UNSET
    approver: MemoryObservationRequestOriginType0ApproverType0 | None | Unset = UNSET
    trace: MemoryObservationRequestOriginType0TraceType0 | None | Unset = UNSET
    span: MemoryObservationRequestOriginType0SpanType0 | None | Unset = UNSET
    workflow: None | str | Unset = UNSET
    workflow_version: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_observation_request_origin_type_0_agent_type_0 import (
            MemoryObservationRequestOriginType0AgentType0,
        )
        from ..models.memory_observation_request_origin_type_0_approver_type_0 import (
            MemoryObservationRequestOriginType0ApproverType0,
        )
        from ..models.memory_observation_request_origin_type_0_span_type_0 import (
            MemoryObservationRequestOriginType0SpanType0,
        )
        from ..models.memory_observation_request_origin_type_0_trace_type_0 import (
            MemoryObservationRequestOriginType0TraceType0,
        )

        agent: dict[str, Any] | None | Unset
        if isinstance(self.agent, Unset):
            agent = UNSET
        elif isinstance(self.agent, MemoryObservationRequestOriginType0AgentType0):
            agent = self.agent.to_dict()
        else:
            agent = self.agent

        approver: dict[str, Any] | None | Unset
        if isinstance(self.approver, Unset):
            approver = UNSET
        elif isinstance(self.approver, MemoryObservationRequestOriginType0ApproverType0):
            approver = self.approver.to_dict()
        else:
            approver = self.approver

        trace: dict[str, Any] | None | Unset
        if isinstance(self.trace, Unset):
            trace = UNSET
        elif isinstance(self.trace, MemoryObservationRequestOriginType0TraceType0):
            trace = self.trace.to_dict()
        else:
            trace = self.trace

        span: dict[str, Any] | None | Unset
        if isinstance(self.span, Unset):
            span = UNSET
        elif isinstance(self.span, MemoryObservationRequestOriginType0SpanType0):
            span = self.span.to_dict()
        else:
            span = self.span

        workflow: None | str | Unset
        if isinstance(self.workflow, Unset):
            workflow = UNSET
        else:
            workflow = self.workflow

        workflow_version: None | str | Unset
        if isinstance(self.workflow_version, Unset):
            workflow_version = UNSET
        else:
            workflow_version = self.workflow_version

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if agent is not UNSET:
            field_dict["agent"] = agent
        if approver is not UNSET:
            field_dict["approver"] = approver
        if trace is not UNSET:
            field_dict["trace"] = trace
        if span is not UNSET:
            field_dict["span"] = span
        if workflow is not UNSET:
            field_dict["workflow"] = workflow
        if workflow_version is not UNSET:
            field_dict["workflow_version"] = workflow_version

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_observation_request_origin_type_0_agent_type_0 import (
            MemoryObservationRequestOriginType0AgentType0,
        )
        from ..models.memory_observation_request_origin_type_0_approver_type_0 import (
            MemoryObservationRequestOriginType0ApproverType0,
        )
        from ..models.memory_observation_request_origin_type_0_span_type_0 import (
            MemoryObservationRequestOriginType0SpanType0,
        )
        from ..models.memory_observation_request_origin_type_0_trace_type_0 import (
            MemoryObservationRequestOriginType0TraceType0,
        )

        d = dict(src_dict)

        def _parse_agent(data: object) -> MemoryObservationRequestOriginType0AgentType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                agent_type_0 = MemoryObservationRequestOriginType0AgentType0.from_dict(data)

                return agent_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryObservationRequestOriginType0AgentType0 | None | Unset, data)

        agent = _parse_agent(d.pop("agent", UNSET))

        def _parse_approver(data: object) -> MemoryObservationRequestOriginType0ApproverType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                approver_type_0 = MemoryObservationRequestOriginType0ApproverType0.from_dict(data)

                return approver_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryObservationRequestOriginType0ApproverType0 | None | Unset, data)

        approver = _parse_approver(d.pop("approver", UNSET))

        def _parse_trace(data: object) -> MemoryObservationRequestOriginType0TraceType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                trace_type_0 = MemoryObservationRequestOriginType0TraceType0.from_dict(data)

                return trace_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryObservationRequestOriginType0TraceType0 | None | Unset, data)

        trace = _parse_trace(d.pop("trace", UNSET))

        def _parse_span(data: object) -> MemoryObservationRequestOriginType0SpanType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                span_type_0 = MemoryObservationRequestOriginType0SpanType0.from_dict(data)

                return span_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryObservationRequestOriginType0SpanType0 | None | Unset, data)

        span = _parse_span(d.pop("span", UNSET))

        def _parse_workflow(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        workflow = _parse_workflow(d.pop("workflow", UNSET))

        def _parse_workflow_version(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        workflow_version = _parse_workflow_version(d.pop("workflow_version", UNSET))

        memory_observation_request_origin_type_0 = cls(
            agent=agent,
            approver=approver,
            trace=trace,
            span=span,
            workflow=workflow,
            workflow_version=workflow_version,
        )

        return memory_observation_request_origin_type_0
