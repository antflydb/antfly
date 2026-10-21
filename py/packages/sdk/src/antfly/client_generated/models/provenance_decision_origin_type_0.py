from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.provenance_decision_origin_type_0_agent_type_0 import ProvenanceDecisionOriginType0AgentType0
    from ..models.provenance_decision_origin_type_0_approver_type_0 import ProvenanceDecisionOriginType0ApproverType0
    from ..models.provenance_decision_origin_type_0_span_type_0 import ProvenanceDecisionOriginType0SpanType0
    from ..models.provenance_decision_origin_type_0_trace_type_0 import ProvenanceDecisionOriginType0TraceType0


T = TypeVar("T", bound="ProvenanceDecisionOriginType0")


@_attrs_define
class ProvenanceDecisionOriginType0:
    """
    Attributes:
        agent (None | ProvenanceDecisionOriginType0AgentType0 | Unset):
        approver (None | ProvenanceDecisionOriginType0ApproverType0 | Unset):
        trace (None | ProvenanceDecisionOriginType0TraceType0 | Unset):
        span (None | ProvenanceDecisionOriginType0SpanType0 | Unset):
        workflow (None | str | Unset):
        workflow_version (None | str | Unset):
    """

    agent: None | ProvenanceDecisionOriginType0AgentType0 | Unset = UNSET
    approver: None | ProvenanceDecisionOriginType0ApproverType0 | Unset = UNSET
    trace: None | ProvenanceDecisionOriginType0TraceType0 | Unset = UNSET
    span: None | ProvenanceDecisionOriginType0SpanType0 | Unset = UNSET
    workflow: None | str | Unset = UNSET
    workflow_version: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.provenance_decision_origin_type_0_agent_type_0 import ProvenanceDecisionOriginType0AgentType0
        from ..models.provenance_decision_origin_type_0_approver_type_0 import (
            ProvenanceDecisionOriginType0ApproverType0,
        )
        from ..models.provenance_decision_origin_type_0_span_type_0 import ProvenanceDecisionOriginType0SpanType0
        from ..models.provenance_decision_origin_type_0_trace_type_0 import ProvenanceDecisionOriginType0TraceType0

        agent: dict[str, Any] | None | Unset
        if isinstance(self.agent, Unset):
            agent = UNSET
        elif isinstance(self.agent, ProvenanceDecisionOriginType0AgentType0):
            agent = self.agent.to_dict()
        else:
            agent = self.agent

        approver: dict[str, Any] | None | Unset
        if isinstance(self.approver, Unset):
            approver = UNSET
        elif isinstance(self.approver, ProvenanceDecisionOriginType0ApproverType0):
            approver = self.approver.to_dict()
        else:
            approver = self.approver

        trace: dict[str, Any] | None | Unset
        if isinstance(self.trace, Unset):
            trace = UNSET
        elif isinstance(self.trace, ProvenanceDecisionOriginType0TraceType0):
            trace = self.trace.to_dict()
        else:
            trace = self.trace

        span: dict[str, Any] | None | Unset
        if isinstance(self.span, Unset):
            span = UNSET
        elif isinstance(self.span, ProvenanceDecisionOriginType0SpanType0):
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
        from ..models.provenance_decision_origin_type_0_agent_type_0 import ProvenanceDecisionOriginType0AgentType0
        from ..models.provenance_decision_origin_type_0_approver_type_0 import (
            ProvenanceDecisionOriginType0ApproverType0,
        )
        from ..models.provenance_decision_origin_type_0_span_type_0 import ProvenanceDecisionOriginType0SpanType0
        from ..models.provenance_decision_origin_type_0_trace_type_0 import ProvenanceDecisionOriginType0TraceType0

        d = dict(src_dict)

        def _parse_agent(data: object) -> None | ProvenanceDecisionOriginType0AgentType0 | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                agent_type_0 = ProvenanceDecisionOriginType0AgentType0.from_dict(data)

                return agent_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | ProvenanceDecisionOriginType0AgentType0 | Unset, data)

        agent = _parse_agent(d.pop("agent", UNSET))

        def _parse_approver(data: object) -> None | ProvenanceDecisionOriginType0ApproverType0 | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                approver_type_0 = ProvenanceDecisionOriginType0ApproverType0.from_dict(data)

                return approver_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | ProvenanceDecisionOriginType0ApproverType0 | Unset, data)

        approver = _parse_approver(d.pop("approver", UNSET))

        def _parse_trace(data: object) -> None | ProvenanceDecisionOriginType0TraceType0 | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                trace_type_0 = ProvenanceDecisionOriginType0TraceType0.from_dict(data)

                return trace_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | ProvenanceDecisionOriginType0TraceType0 | Unset, data)

        trace = _parse_trace(d.pop("trace", UNSET))

        def _parse_span(data: object) -> None | ProvenanceDecisionOriginType0SpanType0 | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                span_type_0 = ProvenanceDecisionOriginType0SpanType0.from_dict(data)

                return span_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | ProvenanceDecisionOriginType0SpanType0 | Unset, data)

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

        provenance_decision_origin_type_0 = cls(
            agent=agent,
            approver=approver,
            trace=trace,
            span=span,
            workflow=workflow,
            workflow_version=workflow_version,
        )

        return provenance_decision_origin_type_0
