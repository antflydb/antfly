from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="GoogleGeneratorConfig")


@_attrs_define
class GoogleGeneratorConfig:
    """Configuration for the Google generative AI provider (Gemini).

    Attributes:
        model (str): The name of the generative model to use. Default: 'gemini-2.5-flash'. Example: gemini-2.5-flash.
        project_id (str | Unset): The Google Cloud project ID.
        location (str | Unset): The Google Cloud location (e.g., 'us-central1').
        temperature (float | Unset): Controls randomness in generation (0.0-2.0).
        max_tokens (int | Unset): Maximum number of tokens to generate.
        top_p (float | Unset): Nucleus sampling parameter.
        top_k (int | Unset): Top-k sampling parameter.
        api_key (str | Unset): The Google API key.
        url (str | Unset): The URL of the Google API endpoint.
    """

    model: str = "gemini-2.5-flash"
    project_id: str | Unset = UNSET
    location: str | Unset = UNSET
    temperature: float | Unset = UNSET
    max_tokens: int | Unset = UNSET
    top_p: float | Unset = UNSET
    top_k: int | Unset = UNSET
    api_key: str | Unset = UNSET
    url: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        project_id = self.project_id

        location = self.location

        temperature = self.temperature

        max_tokens = self.max_tokens

        top_p = self.top_p

        top_k = self.top_k

        api_key = self.api_key

        url = self.url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if project_id is not UNSET:
            field_dict["project_id"] = project_id
        if location is not UNSET:
            field_dict["location"] = location
        if temperature is not UNSET:
            field_dict["temperature"] = temperature
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens
        if top_p is not UNSET:
            field_dict["top_p"] = top_p
        if top_k is not UNSET:
            field_dict["top_k"] = top_k
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if url is not UNSET:
            field_dict["url"] = url

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        project_id = d.pop("project_id", UNSET)

        location = d.pop("location", UNSET)

        temperature = d.pop("temperature", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        top_p = d.pop("top_p", UNSET)

        top_k = d.pop("top_k", UNSET)

        api_key = d.pop("api_key", UNSET)

        url = d.pop("url", UNSET)

        google_generator_config = cls(
            model=model,
            project_id=project_id,
            location=location,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            top_k=top_k,
            api_key=api_key,
            url=url,
        )

        google_generator_config.additional_properties = d
        return google_generator_config

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
