from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AgentContextDiagnostics")


@_attrs_define
class AgentContextDiagnostics:
    """Summary usage is separate from destination generation usage; retries are always zero.

    Attributes:
        contract_version (int | Unset):
        original_input_tokens (int | Unset):
        effective_input_tokens (int | Unset):
        retained_messages (int | Unset):
        compacted_messages (int | Unset):
        batches (int | Unset):
        retries (int | Unset):
        summary_input_tokens (int | Unset):
        summary_output_tokens (int | Unset):
    """

    contract_version: int | Unset = UNSET
    original_input_tokens: int | Unset = UNSET
    effective_input_tokens: int | Unset = UNSET
    retained_messages: int | Unset = UNSET
    compacted_messages: int | Unset = UNSET
    batches: int | Unset = UNSET
    retries: int | Unset = UNSET
    summary_input_tokens: int | Unset = UNSET
    summary_output_tokens: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        contract_version = self.contract_version

        original_input_tokens = self.original_input_tokens

        effective_input_tokens = self.effective_input_tokens

        retained_messages = self.retained_messages

        compacted_messages = self.compacted_messages

        batches = self.batches

        retries = self.retries

        summary_input_tokens = self.summary_input_tokens

        summary_output_tokens = self.summary_output_tokens

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if contract_version is not UNSET:
            field_dict["contract_version"] = contract_version
        if original_input_tokens is not UNSET:
            field_dict["original_input_tokens"] = original_input_tokens
        if effective_input_tokens is not UNSET:
            field_dict["effective_input_tokens"] = effective_input_tokens
        if retained_messages is not UNSET:
            field_dict["retained_messages"] = retained_messages
        if compacted_messages is not UNSET:
            field_dict["compacted_messages"] = compacted_messages
        if batches is not UNSET:
            field_dict["batches"] = batches
        if retries is not UNSET:
            field_dict["retries"] = retries
        if summary_input_tokens is not UNSET:
            field_dict["summary_input_tokens"] = summary_input_tokens
        if summary_output_tokens is not UNSET:
            field_dict["summary_output_tokens"] = summary_output_tokens

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        contract_version = d.pop("contract_version", UNSET)

        original_input_tokens = d.pop("original_input_tokens", UNSET)

        effective_input_tokens = d.pop("effective_input_tokens", UNSET)

        retained_messages = d.pop("retained_messages", UNSET)

        compacted_messages = d.pop("compacted_messages", UNSET)

        batches = d.pop("batches", UNSET)

        retries = d.pop("retries", UNSET)

        summary_input_tokens = d.pop("summary_input_tokens", UNSET)

        summary_output_tokens = d.pop("summary_output_tokens", UNSET)

        agent_context_diagnostics = cls(
            contract_version=contract_version,
            original_input_tokens=original_input_tokens,
            effective_input_tokens=effective_input_tokens,
            retained_messages=retained_messages,
            compacted_messages=compacted_messages,
            batches=batches,
            retries=retries,
            summary_input_tokens=summary_input_tokens,
            summary_output_tokens=summary_output_tokens,
        )

        agent_context_diagnostics.additional_properties = d
        return agent_context_diagnostics

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
