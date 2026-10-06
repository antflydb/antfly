from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chat_gpt_generator_config_provider import ChatGPTGeneratorConfigProvider
from ..models.open_ai_reasoning_effort import OpenAIReasoningEffort
from ..types import UNSET, Unset

T = TypeVar("T", bound="ChatGPTGeneratorConfig")


@_attrs_define
class ChatGPTGeneratorConfig:
    """Personal ChatGPT plan for interactive generation. Credentials stay in the local runtime.

    Attributes:
        provider (ChatGPTGeneratorConfigProvider):
        model (str):
        connection_id (str): Opaque personal registration owned by the authenticated caller.
        reasoning_effort (OpenAIReasoningEffort | Unset): OpenAI reasoning effort; model support varies. Omit to use the
            model default.
    """

    provider: ChatGPTGeneratorConfigProvider
    model: str
    connection_id: str
    reasoning_effort: OpenAIReasoningEffort | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        model = self.model

        connection_id = self.connection_id

        reasoning_effort: str | Unset = UNSET
        if not isinstance(self.reasoning_effort, Unset):
            reasoning_effort = self.reasoning_effort.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
                "model": model,
                "connection_id": connection_id,
            }
        )
        if reasoning_effort is not UNSET:
            field_dict["reasoning_effort"] = reasoning_effort

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = ChatGPTGeneratorConfigProvider(d.pop("provider"))

        model = d.pop("model")

        connection_id = d.pop("connection_id")

        _reasoning_effort = d.pop("reasoning_effort", UNSET)
        reasoning_effort: OpenAIReasoningEffort | Unset
        if isinstance(_reasoning_effort, Unset):
            reasoning_effort = UNSET
        else:
            reasoning_effort = OpenAIReasoningEffort(_reasoning_effort)

        chat_gpt_generator_config = cls(
            provider=provider,
            model=model,
            connection_id=connection_id,
            reasoning_effort=reasoning_effort,
        )

        chat_gpt_generator_config.additional_properties = d
        return chat_gpt_generator_config

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
