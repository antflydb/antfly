from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="BedrockGeneratorConfig")


@_attrs_define
class BedrockGeneratorConfig:
    """Configuration for the AWS Bedrock generative AI provider.

    Provides access to models from Anthropic, Meta, Amazon, Cohere, Mistral, and others.

    **Example Models:** anthropic.claude-sonnet-4-5-20250929-v1:0, meta.llama3-3-70b-instruct-v1:0, amazon.nova-pro-v1:0

    **Docs:** https://docs.aws.amazon.com/bedrock/latest/userguide/models-supported.html

        Attributes:
            model (str): The Bedrock model ID to use (e.g., 'anthropic.claude-sonnet-4-5-20250929-v1:0'). Example:
                anthropic.claude-sonnet-4-5-20250929-v1:0.
            region (Union[Unset, str]): The AWS region for the Bedrock service.
            temperature (Union[Unset, float]): Controls randomness in generation (0.0-1.0).
            max_tokens (Union[Unset, int]): Maximum number of tokens to generate.
            top_p (Union[Unset, float]): Nucleus sampling parameter.
            top_k (Union[Unset, int]): Top-k sampling parameter.
    """

    model: str
    region: Union[Unset, str] = UNSET
    temperature: Union[Unset, float] = UNSET
    max_tokens: Union[Unset, int] = UNSET
    top_p: Union[Unset, float] = UNSET
    top_k: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        region = self.region

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
        if region is not UNSET:
            field_dict["region"] = region
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

        region = d.pop("region", UNSET)

        temperature = d.pop("temperature", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        top_p = d.pop("top_p", UNSET)

        top_k = d.pop("top_k", UNSET)

        bedrock_generator_config = cls(
            model=model,
            region=region,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            top_k=top_k,
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
