from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.ollama_generator_config_provider import OllamaGeneratorConfigProvider
from ..models.open_ai_reasoning_effort import OpenAIReasoningEffort
from ..types import UNSET, Unset

T = TypeVar("T", bound="OllamaGeneratorConfig")


@_attrs_define
class OllamaGeneratorConfig:
    """Configuration for the Ollama generative AI provider.

    Attributes:
        provider (OllamaGeneratorConfigProvider):
        model (str): The name of the Ollama model to use. Example: llama3.3:70b.
        url (str | Unset): The URL of the Ollama API endpoint.
        temperature (float | Unset): Controls randomness in generation (0.0-2.0).
        max_tokens (int | Unset): Maximum number of tokens to generate, including reasoning tokens for thinking models.
        reasoning_effort (OpenAIReasoningEffort | Unset): OpenAI reasoning effort; model support varies. Omit to use the
            model default.
        top_p (float | Unset): Nucleus sampling parameter.
        top_k (int | Unset): Top-k sampling parameter.
        timeout (int | Unset): HTTP response timeout in seconds for Ollama API calls.
    """

    provider: OllamaGeneratorConfigProvider
    model: str
    url: str | Unset = UNSET
    temperature: float | Unset = UNSET
    max_tokens: int | Unset = UNSET
    reasoning_effort: OpenAIReasoningEffort | Unset = UNSET
    top_p: float | Unset = UNSET
    top_k: int | Unset = UNSET
    timeout: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        model = self.model

        url = self.url

        temperature = self.temperature

        max_tokens = self.max_tokens

        reasoning_effort: str | Unset = UNSET
        if not isinstance(self.reasoning_effort, Unset):
            reasoning_effort = self.reasoning_effort.value

        top_p = self.top_p

        top_k = self.top_k

        timeout = self.timeout

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
        if temperature is not UNSET:
            field_dict["temperature"] = temperature
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens
        if reasoning_effort is not UNSET:
            field_dict["reasoning_effort"] = reasoning_effort
        if top_p is not UNSET:
            field_dict["top_p"] = top_p
        if top_k is not UNSET:
            field_dict["top_k"] = top_k
        if timeout is not UNSET:
            field_dict["timeout"] = timeout

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = OllamaGeneratorConfigProvider(d.pop("provider"))

        model = d.pop("model")

        url = d.pop("url", UNSET)

        temperature = d.pop("temperature", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        _reasoning_effort = d.pop("reasoning_effort", UNSET)
        reasoning_effort: OpenAIReasoningEffort | Unset
        if isinstance(_reasoning_effort, Unset):
            reasoning_effort = UNSET
        else:
            reasoning_effort = OpenAIReasoningEffort(_reasoning_effort)

        top_p = d.pop("top_p", UNSET)

        top_k = d.pop("top_k", UNSET)

        timeout = d.pop("timeout", UNSET)

        ollama_generator_config = cls(
            provider=provider,
            model=model,
            url=url,
            temperature=temperature,
            max_tokens=max_tokens,
            reasoning_effort=reasoning_effort,
            top_p=top_p,
            top_k=top_k,
            timeout=timeout,
        )

        ollama_generator_config.additional_properties = d
        return ollama_generator_config

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
