from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.bedrock_generator_config_provider import BedrockGeneratorConfigProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="BedrockGeneratorConfig")


@_attrs_define
class BedrockGeneratorConfig:
    """AWS Bedrock Converse generation, including image inputs and tools for
    models that support them. Uses the AWS default credential chain unless
    an explicit Bedrock API key is configured. Image capability is declared
    per target through inference_capabilities; model IDs are not allowlisted.

        Attributes:
            provider (BedrockGeneratorConfigProvider):
            model (str): The Bedrock model ID to use. Example: anthropic.claude-sonnet-4-5-20250929-v1:0.
            url (str | Unset): Optional Bedrock Runtime endpoint override. Defaults to the regional AWS endpoint.
            api_key (str | Unset): Optional Bedrock bearer API key or secret reference. Omit to use signed AWS requests and
                the default credential chain.
            region (str | Unset): AWS region. Defaults to AWS_REGION, then AWS_DEFAULT_REGION, then us-east-1.
            temperature (float | Unset): Controls randomness in generation (0.0-1.0).
            max_tokens (int | Unset): Maximum number of tokens to generate.
            top_p (float | Unset): Nucleus sampling parameter.
    """

    provider: BedrockGeneratorConfigProvider
    model: str
    url: str | Unset = UNSET
    api_key: str | Unset = UNSET
    region: str | Unset = UNSET
    temperature: float | Unset = UNSET
    max_tokens: int | Unset = UNSET
    top_p: float | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        model = self.model

        url = self.url

        api_key = self.api_key

        region = self.region

        temperature = self.temperature

        max_tokens = self.max_tokens

        top_p = self.top_p

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
        if region is not UNSET:
            field_dict["region"] = region
        if temperature is not UNSET:
            field_dict["temperature"] = temperature
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens
        if top_p is not UNSET:
            field_dict["top_p"] = top_p

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = BedrockGeneratorConfigProvider(d.pop("provider"))

        model = d.pop("model")

        url = d.pop("url", UNSET)

        api_key = d.pop("api_key", UNSET)

        region = d.pop("region", UNSET)

        temperature = d.pop("temperature", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        top_p = d.pop("top_p", UNSET)

        bedrock_generator_config = cls(
            provider=provider,
            model=model,
            url=url,
            api_key=api_key,
            region=region,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
        )

        bedrock_generator_config.additional_properties = d
        return bedrock_generator_config

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
