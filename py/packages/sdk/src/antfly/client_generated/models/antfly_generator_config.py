from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AntflyGeneratorConfig")


@_attrs_define
class AntflyGeneratorConfig:
    """Configuration for the Antfly inference generative AI provider.

    Attributes:
        model (str): The name of the generator model. Example: onnxruntime/Gemma-3-ONNX.
        api_url (str | Unset): The URL of the Inference API endpoint. Can also be set via ANTFLY_INFERENCE_URL
            environment variable.
        temperature (float | Unset): Controls randomness in generation (0.0-2.0).
        max_tokens (int | Unset): Maximum number of tokens to generate.
        top_p (float | Unset): Nucleus sampling parameter.
        top_k (int | Unset): Top-k sampling parameter.
        timeout (int | Unset): HTTP response timeout in seconds for Inference API calls.
    """

    model: str
    api_url: str | Unset = UNSET
    temperature: float | Unset = UNSET
    max_tokens: int | Unset = UNSET
    top_p: float | Unset = UNSET
    top_k: int | Unset = UNSET
    timeout: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        api_url = self.api_url

        temperature = self.temperature

        max_tokens = self.max_tokens

        top_p = self.top_p

        top_k = self.top_k

        timeout = self.timeout

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if api_url is not UNSET:
            field_dict["api_url"] = api_url
        if temperature is not UNSET:
            field_dict["temperature"] = temperature
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens
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
        model = d.pop("model")

        api_url = d.pop("api_url", UNSET)

        temperature = d.pop("temperature", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        top_p = d.pop("top_p", UNSET)

        top_k = d.pop("top_k", UNSET)

        timeout = d.pop("timeout", UNSET)

        antfly_generator_config = cls(
            model=model,
            api_url=api_url,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            top_k=top_k,
            timeout=timeout,
        )

        antfly_generator_config.additional_properties = d
        return antfly_generator_config

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
