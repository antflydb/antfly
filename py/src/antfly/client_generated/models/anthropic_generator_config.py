from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AnthropicGeneratorConfig")


@_attrs_define
class AnthropicGeneratorConfig:
    """Configuration for the Anthropic generative AI provider (Claude models).

    API key via `api_key` field or `ANTHROPIC_API_KEY` environment variable.

    **Example Models:** claude-sonnet-4-5-20250929 (default), claude-opus-4-5-20251101, claude-3-5-haiku-20241022

    **Docs:** https://docs.anthropic.com/en/docs/about-claude/models/overview

        Example:
            {'provider': 'anthropic', 'model': 'claude-sonnet-4-5-20250929', 'temperature': 0.7, 'max_tokens': 4096}

        Attributes:
            model (str): The full model ID of the Anthropic model to use (e.g., 'claude-sonnet-4-5-20250929', 'claude-
                opus-4-5-20251101'). Default: 'claude-sonnet-4-5-20250929'. Example: claude-sonnet-4-5-20250929.
            api_key (Union[Unset, str]): The Anthropic API key. If not provided, falls back to ANTHROPIC_API_KEY environment
                variable.
            url (Union[Unset, str]): The URL of the Anthropic API endpoint (optional, uses default if not specified).
            temperature (Union[Unset, float]): Controls randomness in generation (0.0-1.0). Higher values make output more
                random.
            max_tokens (Union[Unset, int]): Maximum number of tokens to generate in the response.
            top_p (Union[Unset, float]): Nucleus sampling parameter (0.0-1.0). Alternative to temperature.
            top_k (Union[Unset, int]): Top-k sampling parameter. Only sample from the top K options for each subsequent
                token.
    """

    model: str = "claude-sonnet-4-5-20250929"
    api_key: Union[Unset, str] = UNSET
    url: Union[Unset, str] = UNSET
    temperature: Union[Unset, float] = UNSET
    max_tokens: Union[Unset, int] = UNSET
    top_p: Union[Unset, float] = UNSET
    top_k: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        api_key = self.api_key

        url = self.url

        temperature = self.temperature

        max_tokens = self.max_tokens

        top_p = self.top_p

        top_k = self.top_k

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if url is not UNSET:
            field_dict["url"] = url
        if temperature is not UNSET:
            field_dict["temperature"] = temperature
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens
        if top_p is not UNSET:
            field_dict["top_p"] = top_p
        if top_k is not UNSET:
            field_dict["top_k"] = top_k

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        api_key = d.pop("api_key", UNSET)

        url = d.pop("url", UNSET)

        temperature = d.pop("temperature", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        top_p = d.pop("top_p", UNSET)

        top_k = d.pop("top_k", UNSET)

        anthropic_generator_config = cls(
            model=model,
            api_key=api_key,
            url=url,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            top_k=top_k,
        )

        anthropic_generator_config.additional_properties = d
        return anthropic_generator_config

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
