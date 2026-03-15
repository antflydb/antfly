from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="CohereGeneratorConfig")


@_attrs_define
class CohereGeneratorConfig:
    """Configuration for the Cohere generative AI provider (Command models).

    API key via `api_key` field or `COHERE_API_KEY` environment variable.

    **Example Models:** command-r-plus (default), command-r, command-a-03-2025

    **Docs:** https://docs.cohere.com/reference/chat

        Example:
            {'provider': 'cohere', 'model': 'command-r-plus', 'temperature': 0.7, 'max_tokens': 4096}

        Attributes:
            model (str): The name of the Cohere model to use. Default: 'command-r-plus'. Example: command-r-plus.
            api_key (Union[Unset, str]): The Cohere API key. If not provided, falls back to COHERE_API_KEY environment
                variable.
            temperature (Union[Unset, float]): Controls randomness in generation (0.0-1.0). Higher values make output more
                random.
            max_tokens (Union[Unset, int]): Maximum number of tokens to generate in the response.
            top_p (Union[Unset, float]): Nucleus sampling parameter (0.0-1.0). Alternative to temperature.
            top_k (Union[Unset, int]): Top-k sampling parameter. Only sample from the top K options for each subsequent
                token.
            frequency_penalty (Union[Unset, float]): Penalty for token frequency (0.0-1.0).
            presence_penalty (Union[Unset, float]): Penalty for token presence (0.0-1.0).
    """

    model: str = "command-r-plus"
    api_key: Union[Unset, str] = UNSET
    temperature: Union[Unset, float] = UNSET
    max_tokens: Union[Unset, int] = UNSET
    top_p: Union[Unset, float] = UNSET
    top_k: Union[Unset, int] = UNSET
    frequency_penalty: Union[Unset, float] = UNSET
    presence_penalty: Union[Unset, float] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        api_key = self.api_key

        temperature = self.temperature

        max_tokens = self.max_tokens

        top_p = self.top_p

        top_k = self.top_k

        frequency_penalty = self.frequency_penalty

        presence_penalty = self.presence_penalty

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if temperature is not UNSET:
            field_dict["temperature"] = temperature
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens
        if top_p is not UNSET:
            field_dict["top_p"] = top_p
        if top_k is not UNSET:
            field_dict["top_k"] = top_k
        if frequency_penalty is not UNSET:
            field_dict["frequency_penalty"] = frequency_penalty
        if presence_penalty is not UNSET:
            field_dict["presence_penalty"] = presence_penalty

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        api_key = d.pop("api_key", UNSET)

        temperature = d.pop("temperature", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        top_p = d.pop("top_p", UNSET)

        top_k = d.pop("top_k", UNSET)

        frequency_penalty = d.pop("frequency_penalty", UNSET)

        presence_penalty = d.pop("presence_penalty", UNSET)

        cohere_generator_config = cls(
            model=model,
            api_key=api_key,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            top_k=top_k,
            frequency_penalty=frequency_penalty,
            presence_penalty=presence_penalty,
        )

        cohere_generator_config.additional_properties = d
        return cohere_generator_config

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
