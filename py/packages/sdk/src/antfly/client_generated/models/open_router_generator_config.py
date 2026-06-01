from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="OpenRouterGeneratorConfig")


@_attrs_define
class OpenRouterGeneratorConfig:
    """Configuration for the OpenRouter generative AI provider.

    Attributes:
        model (str | Unset): Single model identifier. Either model or models must be provided. Example: openai/gpt-4.1.
        models (list[str] | Unset): Array of model identifiers for fallback routing. Either model or models must be
            provided.
        api_key (str | Unset): The OpenRouter API key.
        temperature (float | Unset): Controls randomness in generation (0.0-2.0).
        max_tokens (int | Unset): Maximum number of tokens to generate in the response.
        top_p (float | Unset): Nucleus sampling parameter (0.0-1.0).
        frequency_penalty (float | Unset): Penalty for token frequency (-2.0 to 2.0).
        presence_penalty (float | Unset): Penalty for token presence (-2.0 to 2.0).
    """

    model: str | Unset = UNSET
    models: list[str] | Unset = UNSET
    api_key: str | Unset = UNSET
    temperature: float | Unset = UNSET
    max_tokens: int | Unset = UNSET
    top_p: float | Unset = UNSET
    frequency_penalty: float | Unset = UNSET
    presence_penalty: float | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        models: list[str] | Unset = UNSET
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
