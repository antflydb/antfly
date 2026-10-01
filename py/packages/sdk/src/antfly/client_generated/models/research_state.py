from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.research_phase import ResearchPhase
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.research_citation import ResearchCitation
    from ..models.research_evidence import ResearchEvidence
    from ..models.research_finding import ResearchFinding
    from ..models.research_plan import ResearchPlan
    from ..models.research_reflection import ResearchReflection
    from ..models.research_report import ResearchReport
    from ..models.research_usage import ResearchUsage
    from ..models.research_verification import ResearchVerification


T = TypeVar("T", bound="ResearchState")


@_attrs_define
class ResearchState:
    """Client-carried continuation state. Sending it back resumes the run at
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

        Attributes:
            phase (ResearchPhase): Research state-machine phase. `plan` decomposes the question,
                `research` runs one bounded round of retrieval researchers, `reflect`
                decides whether another round is needed, `write` produces the cited
                report, `verify` checks citations, and `done` is terminal.
            signature (str | Unset): Server signature over this state. Do not modify the state.
            round_ (int | Unset): Completed research rounds.
            plan (ResearchPlan | Unset):
            findings (list[ResearchFinding] | Unset):
            evidence (list[ResearchEvidence] | Unset):
            reflections (list[ResearchReflection] | Unset):
            report (ResearchReport | Unset):
            citations (list[ResearchCitation] | Unset):
            verification (ResearchVerification | Unset):
            usage (ResearchUsage | Unset):
    """

    phase: ResearchPhase
    signature: str | Unset = UNSET
    round_: int | Unset = UNSET
    plan: ResearchPlan | Unset = UNSET
    findings: list[ResearchFinding] | Unset = UNSET
    evidence: list[ResearchEvidence] | Unset = UNSET
    reflections: list[ResearchReflection] | Unset = UNSET
    report: ResearchReport | Unset = UNSET
    citations: list[ResearchCitation] | Unset = UNSET
    verification: ResearchVerification | Unset = UNSET
    usage: ResearchUsage | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        phase = self.phase.value

        signature = self.signature

        round_ = self.round_

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

        usage: dict[str, Any] | Unset = UNSET
        if not isinstance(self.usage, Unset):
            usage = self.usage.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "phase": phase,
            }
        )
        if signature is not UNSET:
            field_dict["signature"] = signature
        if round_ is not UNSET:
            field_dict["round"] = round_
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
        if usage is not UNSET:
            field_dict["usage"] = usage

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.research_citation import ResearchCitation
        from ..models.research_evidence import ResearchEvidence
        from ..models.research_finding import ResearchFinding
        from ..models.research_plan import ResearchPlan
        from ..models.research_reflection import ResearchReflection
        from ..models.research_report import ResearchReport
        from ..models.research_usage import ResearchUsage
        from ..models.research_verification import ResearchVerification

        d = dict(src_dict)
        phase = ResearchPhase(d.pop("phase"))

        signature = d.pop("signature", UNSET)

        round_ = d.pop("round", UNSET)

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

        _usage = d.pop("usage", UNSET)
        usage: ResearchUsage | Unset
        if isinstance(_usage, Unset):
            usage = UNSET
        else:
            usage = ResearchUsage.from_dict(_usage)

        research_state = cls(
            phase=phase,
            signature=signature,
            round_=round_,
            plan=plan,
            findings=findings,
            evidence=evidence,
            reflections=reflections,
            report=report,
            citations=citations,
            verification=verification,
            usage=usage,
        )

        research_state.additional_properties = d
        return research_state

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
