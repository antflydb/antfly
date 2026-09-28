from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.research_job_state import ResearchJobState
from ..models.research_phase import ResearchPhase
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.research_agent_result import ResearchAgentResult


T = TypeVar("T", bound="ResearchJob")


@_attrs_define
class ResearchJob:
    """
    Attributes:
        job_id (str): Job identifier.
        state (ResearchJobState): Durable research job lifecycle state.
        phase (ResearchPhase): Research state-machine phase. `plan` decomposes the question,
            `research` runs one bounded round of retrieval researchers, `reflect`
            decides whether another round is needed, `write` produces the cited
            report, `verify` checks citations, and `done` is terminal.
        query (str | Unset): The research question.
        advances (int | Unset): Completed advance calls.
        cancel_requested (bool | Unset): Whether cancellation was requested.
        last_error (str | Unset): Last advance error, if any.
        created_at_ms (int | Unset):
        updated_at_ms (int | Unset):
        expires_at_ms (int | Unset):
        result (ResearchAgentResult | Unset): Result from the research agent.
    """

    job_id: str
    state: ResearchJobState
    phase: ResearchPhase
    query: str | Unset = UNSET
    advances: int | Unset = UNSET
    cancel_requested: bool | Unset = UNSET
    last_error: str | Unset = UNSET
    created_at_ms: int | Unset = UNSET
    updated_at_ms: int | Unset = UNSET
    expires_at_ms: int | Unset = UNSET
    result: ResearchAgentResult | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        job_id = self.job_id

        state = self.state.value

        phase = self.phase.value

        query = self.query

        advances = self.advances

        cancel_requested = self.cancel_requested

        last_error = self.last_error

        created_at_ms = self.created_at_ms

        updated_at_ms = self.updated_at_ms

        expires_at_ms = self.expires_at_ms

        result: dict[str, Any] | Unset = UNSET
        if not isinstance(self.result, Unset):
            result = self.result.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "job_id": job_id,
                "state": state,
                "phase": phase,
            }
        )
        if query is not UNSET:
            field_dict["query"] = query
        if advances is not UNSET:
            field_dict["advances"] = advances
        if cancel_requested is not UNSET:
            field_dict["cancel_requested"] = cancel_requested
        if last_error is not UNSET:
            field_dict["last_error"] = last_error
        if created_at_ms is not UNSET:
            field_dict["created_at_ms"] = created_at_ms
        if updated_at_ms is not UNSET:
            field_dict["updated_at_ms"] = updated_at_ms
        if expires_at_ms is not UNSET:
            field_dict["expires_at_ms"] = expires_at_ms
        if result is not UNSET:
            field_dict["result"] = result

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.research_agent_result import ResearchAgentResult

        d = dict(src_dict)
        job_id = d.pop("job_id")

        state = ResearchJobState(d.pop("state"))

        phase = ResearchPhase(d.pop("phase"))

        query = d.pop("query", UNSET)

        advances = d.pop("advances", UNSET)

        cancel_requested = d.pop("cancel_requested", UNSET)

        last_error = d.pop("last_error", UNSET)

        created_at_ms = d.pop("created_at_ms", UNSET)

        updated_at_ms = d.pop("updated_at_ms", UNSET)

        expires_at_ms = d.pop("expires_at_ms", UNSET)

        _result = d.pop("result", UNSET)
        result: ResearchAgentResult | Unset
        if isinstance(_result, Unset):
            result = UNSET
        else:
            result = ResearchAgentResult.from_dict(_result)

        research_job = cls(
            job_id=job_id,
            state=state,
            phase=phase,
            query=query,
            advances=advances,
            cancel_requested=cancel_requested,
            last_error=last_error,
            created_at_ms=created_at_ms,
            updated_at_ms=updated_at_ms,
            expires_at_ms=expires_at_ms,
            result=result,
        )

        research_job.additional_properties = d
        return research_job

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
