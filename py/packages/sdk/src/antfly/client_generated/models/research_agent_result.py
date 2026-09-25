from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.agent_status import AgentStatus
from ..models.research_phase import ResearchPhase
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.agent_question import AgentQuestion
    from ..models.agent_step import AgentStep
    from ..models.research_citation import ResearchCitation
    from ..models.research_evidence import ResearchEvidence
    from ..models.research_finding import ResearchFinding
    from ..models.research_incomplete_details import ResearchIncompleteDetails
    from ..models.research_plan import ResearchPlan
    from ..models.research_reflection import ResearchReflection
    from ..models.research_report import ResearchReport
    from ..models.research_state import ResearchState
    from ..models.research_usage import ResearchUsage
    from ..models.research_verification import ResearchVerification


T = TypeVar("T", bound="ResearchAgentResult")


@_attrs_define
class ResearchAgentResult:
    """Result from the research agent.

    Attributes:
        status (AgentStatus): Shared bounded-agent execution status
        research_state (ResearchState): Client-carried continuation state. Sending it back resumes the run at
            `phase` without repeating completed work. It never contains raw tool
            transcripts, credentials or connection settings. Evidence snippets are
            bounded excerpts of documents the caller was authorized to read; every
            resumed request is re-authorized.

            The server signs the state it returns (`signature`) and rejects a
            state whose signature does not verify, so a client cannot alter a
            checkpoint, including its budget counters. Send the state back
            unmodified. Signatures are valid across a cluster that shares an
            internal service secret, otherwise only on the server that issued them
            and until it restarts; use durable jobs to resume across restarts.
        id (str | Unset): Unique response ID. Example: resr_cr3ig20h5tbs73e3ahrg.
        model (str | Unset): Writer model.
        created_at (int | Unset): Unix timestamp (seconds) when the response was created.
        incomplete_details (ResearchIncompleteDetails | Unset):
        phase (ResearchPhase | Unset): Research state-machine phase. `plan` decomposes the question,
            `research` runs one bounded round of retrieval researchers, `reflect`
            decides whether another round is needed, `write` produces the cited
            report, `verify` checks citations, and `done` is terminal.
        usage (ResearchUsage | Unset):
        plan (ResearchPlan | Unset):
        findings (list[ResearchFinding] | Unset):
        evidence (list[ResearchEvidence] | Unset):
        reflections (list[ResearchReflection] | Unset):
        report (ResearchReport | Unset):
        citations (list[ResearchCitation] | Unset):
        verification (ResearchVerification | Unset):
        steps (list[AgentStep] | Unset): Execution trace.
        questions (list[AgentQuestion] | Unset): Clarification questions when status is clarification_required.
        session_id (str | Unset): Echoed correlation identifier.
    """

    status: AgentStatus
    research_state: ResearchState
    id: str | Unset = UNSET
    model: str | Unset = UNSET
    created_at: int | Unset = UNSET
    incomplete_details: ResearchIncompleteDetails | Unset = UNSET
    phase: ResearchPhase | Unset = UNSET
    usage: ResearchUsage | Unset = UNSET
    plan: ResearchPlan | Unset = UNSET
    findings: list[ResearchFinding] | Unset = UNSET
    evidence: list[ResearchEvidence] | Unset = UNSET
    reflections: list[ResearchReflection] | Unset = UNSET
    report: ResearchReport | Unset = UNSET
    citations: list[ResearchCitation] | Unset = UNSET
    verification: ResearchVerification | Unset = UNSET
    steps: list[AgentStep] | Unset = UNSET
    questions: list[AgentQuestion] | Unset = UNSET
    session_id: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        research_state = self.research_state.to_dict()

        id = self.id

        model = self.model

        created_at = self.created_at

        incomplete_details: dict[str, Any] | Unset = UNSET
        if not isinstance(self.incomplete_details, Unset):
            incomplete_details = self.incomplete_details.to_dict()

        phase: str | Unset = UNSET
        if not isinstance(self.phase, Unset):
            phase = self.phase.value

        usage: dict[str, Any] | Unset = UNSET
        if not isinstance(self.usage, Unset):
            usage = self.usage.to_dict()

        plan: dict[str, Any] | Unset = UNSET
        if not isinstance(self.plan, Unset):
            plan = self.plan.to_dict()

        findings: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.findings, Unset):
            findings = []
            for findings_item_data in self.findings:
                findings_item = findings_item_data.to_dict()
                findings.append(findings_item)

        evidence: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.evidence, Unset):
            evidence = []
            for evidence_item_data in self.evidence:
                evidence_item = evidence_item_data.to_dict()
                evidence.append(evidence_item)

        reflections: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.reflections, Unset):
            reflections = []
            for reflections_item_data in self.reflections:
                reflections_item = reflections_item_data.to_dict()
                reflections.append(reflections_item)

        report: dict[str, Any] | Unset = UNSET
        if not isinstance(self.report, Unset):
            report = self.report.to_dict()

        citations: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.citations, Unset):
            citations = []
            for citations_item_data in self.citations:
                citations_item = citations_item_data.to_dict()
                citations.append(citations_item)

        verification: dict[str, Any] | Unset = UNSET
        if not isinstance(self.verification, Unset):
            verification = self.verification.to_dict()

        steps: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.steps, Unset):
            steps = []
            for steps_item_data in self.steps:
                steps_item = steps_item_data.to_dict()
                steps.append(steps_item)

        questions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.questions, Unset):
            questions = []
            for questions_item_data in self.questions:
                questions_item = questions_item_data.to_dict()
                questions.append(questions_item)

        session_id = self.session_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "status": status,
                "research_state": research_state,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id
        if model is not UNSET:
            field_dict["model"] = model
        if created_at is not UNSET:
            field_dict["created_at"] = created_at
        if incomplete_details is not UNSET:
            field_dict["incomplete_details"] = incomplete_details
        if phase is not UNSET:
            field_dict["phase"] = phase
        if usage is not UNSET:
            field_dict["usage"] = usage
        if plan is not UNSET:
            field_dict["plan"] = plan
        if findings is not UNSET:
            field_dict["findings"] = findings
        if evidence is not UNSET:
            field_dict["evidence"] = evidence
        if reflections is not UNSET:
            field_dict["reflections"] = reflections
        if report is not UNSET:
            field_dict["report"] = report
        if citations is not UNSET:
            field_dict["citations"] = citations
        if verification is not UNSET:
            field_dict["verification"] = verification
        if steps is not UNSET:
            field_dict["steps"] = steps
        if questions is not UNSET:
            field_dict["questions"] = questions
        if session_id is not UNSET:
            field_dict["session_id"] = session_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.agent_question import AgentQuestion
        from ..models.agent_step import AgentStep
        from ..models.research_citation import ResearchCitation
        from ..models.research_evidence import ResearchEvidence
        from ..models.research_finding import ResearchFinding
        from ..models.research_incomplete_details import ResearchIncompleteDetails
        from ..models.research_plan import ResearchPlan
        from ..models.research_reflection import ResearchReflection
        from ..models.research_report import ResearchReport
        from ..models.research_state import ResearchState
        from ..models.research_usage import ResearchUsage
        from ..models.research_verification import ResearchVerification

        d = dict(src_dict)
        status = AgentStatus(d.pop("status"))

        research_state = ResearchState.from_dict(d.pop("research_state"))

        id = d.pop("id", UNSET)

        model = d.pop("model", UNSET)

        created_at = d.pop("created_at", UNSET)

        _incomplete_details = d.pop("incomplete_details", UNSET)
        incomplete_details: ResearchIncompleteDetails | Unset
        if isinstance(_incomplete_details, Unset):
            incomplete_details = UNSET
        else:
            incomplete_details = ResearchIncompleteDetails.from_dict(_incomplete_details)

        _phase = d.pop("phase", UNSET)
        phase: ResearchPhase | Unset
        if isinstance(_phase, Unset):
            phase = UNSET
        else:
            phase = ResearchPhase(_phase)

        _usage = d.pop("usage", UNSET)
        usage: ResearchUsage | Unset
        if isinstance(_usage, Unset):
            usage = UNSET
        else:
            usage = ResearchUsage.from_dict(_usage)

        _plan = d.pop("plan", UNSET)
        plan: ResearchPlan | Unset
        if isinstance(_plan, Unset):
            plan = UNSET
        else:
            plan = ResearchPlan.from_dict(_plan)

        _findings = d.pop("findings", UNSET)
        findings: list[ResearchFinding] | Unset = UNSET
        if _findings is not UNSET:
            findings = []
            for findings_item_data in _findings:
                findings_item = ResearchFinding.from_dict(findings_item_data)

                findings.append(findings_item)

        _evidence = d.pop("evidence", UNSET)
        evidence: list[ResearchEvidence] | Unset = UNSET
        if _evidence is not UNSET:
            evidence = []
            for evidence_item_data in _evidence:
                evidence_item = ResearchEvidence.from_dict(evidence_item_data)

                evidence.append(evidence_item)

        _reflections = d.pop("reflections", UNSET)
        reflections: list[ResearchReflection] | Unset = UNSET
        if _reflections is not UNSET:
            reflections = []
            for reflections_item_data in _reflections:
                reflections_item = ResearchReflection.from_dict(reflections_item_data)

                reflections.append(reflections_item)

        _report = d.pop("report", UNSET)
        report: ResearchReport | Unset
        if isinstance(_report, Unset):
            report = UNSET
        else:
            report = ResearchReport.from_dict(_report)

        _citations = d.pop("citations", UNSET)
        citations: list[ResearchCitation] | Unset = UNSET
        if _citations is not UNSET:
            citations = []
            for citations_item_data in _citations:
                citations_item = ResearchCitation.from_dict(citations_item_data)

                citations.append(citations_item)

        _verification = d.pop("verification", UNSET)
        verification: ResearchVerification | Unset
        if isinstance(_verification, Unset):
            verification = UNSET
        else:
            verification = ResearchVerification.from_dict(_verification)

        _steps = d.pop("steps", UNSET)
        steps: list[AgentStep] | Unset = UNSET
        if _steps is not UNSET:
            steps = []
            for steps_item_data in _steps:
                steps_item = AgentStep.from_dict(steps_item_data)

                steps.append(steps_item)

        _questions = d.pop("questions", UNSET)
        questions: list[AgentQuestion] | Unset = UNSET
        if _questions is not UNSET:
            questions = []
            for questions_item_data in _questions:
                questions_item = AgentQuestion.from_dict(questions_item_data)

                questions.append(questions_item)

        session_id = d.pop("session_id", UNSET)

        research_agent_result = cls(
            status=status,
            research_state=research_state,
            id=id,
            model=model,
            created_at=created_at,
            incomplete_details=incomplete_details,
            phase=phase,
            usage=usage,
            plan=plan,
            findings=findings,
            evidence=evidence,
            reflections=reflections,
            report=report,
            citations=citations,
            verification=verification,
            steps=steps,
            questions=questions,
            session_id=session_id,
        )

        research_agent_result.additional_properties = d
        return research_agent_result

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
