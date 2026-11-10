from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.agent_context_diagnostics import AgentContextDiagnostics


T = TypeVar("T", bound="AgentContextEvent")


@_attrs_define
class AgentContextEvent:
    """
    Attributes:
        counting_method (str):
        latency_ms (int):
        diagnostics (AgentContextDiagnostics): Summary usage is separate from destination generation usage; retries are
            always zero.
        summary_provider (str | Unset):
        summary_model (str | Unset):
    """

    counting_method: str
    latency_ms: int
    diagnostics: AgentContextDiagnostics
    summary_provider: str | Unset = UNSET
    summary_model: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        counting_method = self.counting_method

        latency_ms = self.latency_ms

        diagnostics = self.diagnostics.to_dict()

        summary_provider = self.summary_provider

        summary_model = self.summary_model

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "counting_method": counting_method,
                "latency_ms": latency_ms,
                "diagnostics": diagnostics,
            }
        )
        if summary_provider is not UNSET:
            field_dict["summary_provider"] = summary_provider
        if summary_model is not UNSET:
            field_dict["summary_model"] = summary_model

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.agent_context_diagnostics import AgentContextDiagnostics

        d = dict(src_dict)
        counting_method = d.pop("counting_method")

        latency_ms = d.pop("latency_ms")

        diagnostics = AgentContextDiagnostics.from_dict(d.pop("diagnostics"))

        summary_provider = d.pop("summary_provider", UNSET)

        summary_model = d.pop("summary_model", UNSET)

        agent_context_event = cls(
            counting_method=counting_method,
            latency_ms=latency_ms,
            diagnostics=diagnostics,
            summary_provider=summary_provider,
            summary_model=summary_model,
        )

        agent_context_event.additional_properties = d
        return agent_context_event

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
