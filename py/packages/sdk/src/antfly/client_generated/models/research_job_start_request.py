from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.research_agent_request import ResearchAgentRequest


T = TypeVar("T", bound="ResearchJobStartRequest")


@_attrs_define
class ResearchJobStartRequest:
    """
    Attributes:
        request (ResearchAgentRequest): Request for the research agent. The agent plans sub-questions, runs a
            bounded retrieval researcher per sub-question in parallel, reflects on
            coverage, and writes a long-form report whose `[E#]` citations resolve
            to a deduplicated evidence registry.

            Researchers are ordinary retrieval-agent runs over `queries` with the
            same authorization, mandatory predicates and tool policy. They cannot
            widen tables, filters, tools or budgets.
        advance (int | Unset): Number of phases to run before the start call returns. Default: 0.
    """

    request: ResearchAgentRequest
    advance: int | Unset = 0
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        request = self.request.to_dict()

        advance = self.advance

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "request": request,
            }
        )
        if advance is not UNSET:
            field_dict["advance"] = advance

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.research_agent_request import ResearchAgentRequest

        d = dict(src_dict)
        request = ResearchAgentRequest.from_dict(d.pop("request"))

        advance = d.pop("advance", UNSET)

        research_job_start_request = cls(
            request=request,
            advance=advance,
        )

        research_job_start_request.additional_properties = d
        return research_job_start_request

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
