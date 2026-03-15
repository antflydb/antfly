from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="OllamaGeneratorConfig")


@_attrs_define
class OllamaGeneratorConfig:
    """Configuration for the Ollama generative AI provider.

    Ollama provides local LLM inference for privacy and offline use.

    **Example Models:** llama3.3:70b, qwen2.5:72b, deepseek-r1:70b, mistral:7b, llava:34b

    **Docs:** https://ollama.com/library

        Attributes:
            model (str): The name of the Ollama model to use (e.g., 'llama3.3:70b', 'qwen2.5:72b', 'deepseek-coder:33b').
                Example: llama3.3:70b.
            url (Union[Unset, str]): The URL of the Ollama API endpoint.
            temperature (Union[Unset, float]): Controls randomness in generation (0.0-2.0).
            max_tokens (Union[Unset, int]): Maximum number of tokens to generate.
            top_p (Union[Unset, float]): Nucleus sampling parameter.
            top_k (Union[Unset, int]): Top-k sampling parameter.
            timeout (Union[Unset, int]): HTTP response timeout in seconds for Ollama API calls. Defaults to 540 (9 minutes).
                Increase for large models on slow hardware.
    """

    model: str
    url: Union[Unset, str] = UNSET
    temperature: Union[Unset, float] = UNSET
    max_tokens: Union[Unset, int] = UNSET
    top_p: Union[Unset, float] = UNSET
    top_k: Union[Unset, int] = UNSET
    timeout: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        url = self.url

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
        if timeout is not UNSET:
            field_dict["timeout"] = timeout

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        url = d.pop("url", UNSET)

        temperature = d.pop("temperature", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        top_p = d.pop("top_p", UNSET)

        top_k = d.pop("top_k", UNSET)

        timeout = d.pop("timeout", UNSET)

        ollama_generator_config = cls(
            model=model,
            url=url,
            temperature=temperature,
            max_tokens=max_tokens,
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
