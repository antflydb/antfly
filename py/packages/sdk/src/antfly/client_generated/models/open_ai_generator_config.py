from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.open_ai_generator_config_provider import OpenAIGeneratorConfigProvider
from ..models.open_ai_reasoning_effort import OpenAIReasoningEffort
from ..types import UNSET, Unset

T = TypeVar("T", bound="OpenAIGeneratorConfig")


@_attrs_define
class OpenAIGeneratorConfig:
    """Configuration for the OpenAI generative AI provider.

    Attributes:
        provider (OpenAIGeneratorConfigProvider):
        model (str): The name of the OpenAI model to use. Default: 'gpt-4.1'. Example: gpt-4.1.
        url (str | Unset): The URL of the OpenAI API endpoint.
        api_key (str | Unset): The OpenAI API key.
        temperature (float | Unset): Controls randomness in generation (0.0-2.0).
        max_tokens (int | Unset): Maximum number of tokens to generate.
        max_completion_tokens (int | Unset): OpenAI completion budget, including visible output and reasoning tokens.
            Use for reasoning models instead of max_tokens; the two are mutually exclusive.
        reasoning_effort (OpenAIReasoningEffort | Unset): OpenAI reasoning effort; model support varies. Omit to use the
            model default.
        top_p (float | Unset): Nucleus sampling parameter.
        frequency_penalty (float | Unset): Penalty for token frequency (-2.0 to 2.0).
        presence_penalty (float | Unset): Penalty for token presence (-2.0 to 2.0).
    """

    provider: OpenAIGeneratorConfigProvider
    model: str = "gpt-4.1"
    url: str | Unset = UNSET
    api_key: str | Unset = UNSET
    temperature: float | Unset = UNSET
    max_tokens: int | Unset = UNSET
    max_completion_tokens: int | Unset = UNSET
    reasoning_effort: OpenAIReasoningEffort | Unset = UNSET
    top_p: float | Unset = UNSET
    frequency_penalty: float | Unset = UNSET
    presence_penalty: float | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        model = self.model

        url = self.url

        api_key = self.api_key

        temperature = self.temperature

        max_tokens = self.max_tokens

        max_completion_tokens = self.max_completion_tokens

        reasoning_effort: str | Unset = UNSET
        if not isinstance(self.reasoning_effort, Unset):
            reasoning_effort = self.reasoning_effort.value

        top_p = self.top_p

        frequency_penalty = self.frequency_penalty

        presence_penalty = self.presence_penalty

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
                "model": model,
            }
        )
        if url is not UNSET:
            field_dict["url"] = url
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if temperature is not UNSET:
            field_dict["temperature"] = temperature
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens
        if max_completion_tokens is not UNSET:
            field_dict["max_completion_tokens"] = max_completion_tokens
        if reasoning_effort is not UNSET:
            field_dict["reasoning_effort"] = reasoning_effort
        if top_p is not UNSET:
            field_dict["top_p"] = top_p
        if frequency_penalty is not UNSET:
            field_dict["frequency_penalty"] = frequency_penalty
        if presence_penalty is not UNSET:
            field_dict["presence_penalty"] = presence_penalty

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = OpenAIGeneratorConfigProvider(d.pop("provider"))

        model = d.pop("model")

        url = d.pop("url", UNSET)

        api_key = d.pop("api_key", UNSET)

        temperature = d.pop("temperature", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        max_completion_tokens = d.pop("max_completion_tokens", UNSET)

        _reasoning_effort = d.pop("reasoning_effort", UNSET)
        reasoning_effort: OpenAIReasoningEffort | Unset
        if isinstance(_reasoning_effort, Unset):
            reasoning_effort = UNSET
        else:
            reasoning_effort = OpenAIReasoningEffort(_reasoning_effort)

        top_p = d.pop("top_p", UNSET)

        frequency_penalty = d.pop("frequency_penalty", UNSET)

        presence_penalty = d.pop("presence_penalty", UNSET)

        open_ai_generator_config = cls(
            provider=provider,
            model=model,
            url=url,
            api_key=api_key,
            temperature=temperature,
            max_tokens=max_tokens,
            max_completion_tokens=max_completion_tokens,
            reasoning_effort=reasoning_effort,
            top_p=top_p,
            frequency_penalty=frequency_penalty,
            presence_penalty=presence_penalty,
        )

        open_ai_generator_config.additional_properties = d
        return open_ai_generator_config

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
