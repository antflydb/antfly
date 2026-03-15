from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="OpenRouterGeneratorConfig")


@_attrs_define
class OpenRouterGeneratorConfig:
    """Configuration for the OpenRouter generative AI provider.

    OpenRouter provides a unified API for multiple LLM providers with automatic fallback routing.
    API key via `api_key` field or `OPENROUTER_API_KEY` environment variable.

    **Model Selection:**
    - Use `model` for a single model (e.g., "openai/gpt-4.1", "anthropic/claude-sonnet-4-5-20250929")
    - Use `models` array for fallback routing - OpenRouter tries models in order until one succeeds

    **Example Models:** openai/gpt-4.1, anthropic/claude-sonnet-4-5-20250929, google/gemini-2.5-flash,
    meta-llama/llama-3.3-70b-instruct

    **Docs:** https://openrouter.ai/docs/api/api-reference/chat/send-chat-completion-request

        Example:
            {'provider': 'openrouter', 'model': 'openai/gpt-4.1', 'temperature': 0.7, 'max_tokens': 4096}

        Attributes:
            model (Union[Unset, str]): Single model identifier (e.g., 'openai/gpt-4.1'). Either model or models must be
                provided. Example: openai/gpt-4.1.
            models (Union[Unset, list[str]]): Array of model identifiers for fallback routing. OpenRouter tries each model
                in order
                until one succeeds. Either model or models must be provided.
                 Example: ['openai/gpt-4.1', 'anthropic/claude-sonnet-4-5-20250929', 'google/gemini-2.5-flash'].
            api_key (Union[Unset, str]): The OpenRouter API key. Can also be set via OPENROUTER_API_KEY environment
                variable.
            temperature (Union[Unset, float]): Controls randomness in generation (0.0-2.0). Higher values make output more
                random.
            max_tokens (Union[Unset, int]): Maximum number of tokens to generate in the response.
            top_p (Union[Unset, float]): Nucleus sampling parameter (0.0-1.0). Alternative to temperature.
            frequency_penalty (Union[Unset, float]): Penalty for token frequency (-2.0 to 2.0).
            presence_penalty (Union[Unset, float]): Penalty for token presence (-2.0 to 2.0).
    """

    model: Union[Unset, str] = UNSET
    models: Union[Unset, list[str]] = UNSET
    api_key: Union[Unset, str] = UNSET
    temperature: Union[Unset, float] = UNSET
    max_tokens: Union[Unset, int] = UNSET
    top_p: Union[Unset, float] = UNSET
    frequency_penalty: Union[Unset, float] = UNSET
    presence_penalty: Union[Unset, float] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        models: Union[Unset, list[str]] = UNSET
        if not isinstance(self.models, Unset):
            models = self.models

        api_key = self.api_key

        temperature = self.temperature

        max_tokens = self.max_tokens

        top_p = self.top_p

        frequency_penalty = self.frequency_penalty

        presence_penalty = self.presence_penalty

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if model is not UNSET:
            field_dict["model"] = model
        if models is not UNSET:
            field_dict["models"] = models
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if temperature is not UNSET:
            field_dict["temperature"] = temperature
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens
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
        model = d.pop("model", UNSET)

        models = cast(list[str], d.pop("models", UNSET))

        api_key = d.pop("api_key", UNSET)

        temperature = d.pop("temperature", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        top_p = d.pop("top_p", UNSET)

        frequency_penalty = d.pop("frequency_penalty", UNSET)

        presence_penalty = d.pop("presence_penalty", UNSET)

        open_router_generator_config = cls(
            model=model,
            models=models,
            api_key=api_key,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            frequency_penalty=frequency_penalty,
            presence_penalty=presence_penalty,
        )

        open_router_generator_config.additional_properties = d
        return open_router_generator_config

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
