from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="OpenAIGeneratorConfig")


@_attrs_define
class OpenAIGeneratorConfig:
    """Configuration for the OpenAI generative AI provider.

    **Example Models:** gpt-4.1 (default), gpt-4.1-mini, o3, o4-mini

    **Docs:** https://platform.openai.com/docs/models

        Attributes:
            model (str): The name of the OpenAI model to use (e.g., 'gpt-4.1', 'gpt-4.1-mini', 'o4-mini'). Default:
                'gpt-4.1'. Example: gpt-4.1.
            url (Union[Unset, str]): The URL of the OpenAI API endpoint.
            api_key (Union[Unset, str]): The OpenAI API key.
            temperature (Union[Unset, float]): Controls randomness in generation (0.0-2.0).
            max_tokens (Union[Unset, int]): Maximum number of tokens to generate.
            top_p (Union[Unset, float]): Nucleus sampling parameter.
            frequency_penalty (Union[Unset, float]): Penalty for token frequency (-2.0 to 2.0).
            presence_penalty (Union[Unset, float]): Penalty for token presence (-2.0 to 2.0).
    """

    model: str = "gpt-4.1"
    url: Union[Unset, str] = UNSET
    api_key: Union[Unset, str] = UNSET
    temperature: Union[Unset, float] = UNSET
    max_tokens: Union[Unset, int] = UNSET
    top_p: Union[Unset, float] = UNSET
    frequency_penalty: Union[Unset, float] = UNSET
    presence_penalty: Union[Unset, float] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        url = self.url

        api_key = self.api_key

        temperature = self.temperature

        max_tokens = self.max_tokens

        top_p = self.top_p

        frequency_penalty = self.frequency_penalty

        presence_penalty = self.presence_penalty

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
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
        model = d.pop("model")

        url = d.pop("url", UNSET)

        api_key = d.pop("api_key", UNSET)

        temperature = d.pop("temperature", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        top_p = d.pop("top_p", UNSET)

        frequency_penalty = d.pop("frequency_penalty", UNSET)

        presence_penalty = d.pop("presence_penalty", UNSET)

        open_ai_generator_config = cls(
            model=model,
            url=url,
            api_key=api_key,
            temperature=temperature,
            max_tokens=max_tokens,
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
