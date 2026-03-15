from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="GoogleGeneratorConfig")


@_attrs_define
class GoogleGeneratorConfig:
    """Configuration for the Google generative AI provider (Gemini).

    **Example Models:** gemini-2.5-flash (default), gemini-2.5-pro, gemini-3.0-pro

    **Docs:** https://ai.google.dev/gemini-api/docs/models

        Attributes:
            model (str): The name of the generative model to use (e.g., 'gemini-2.5-flash', 'gemini-2.5-pro',
                'gemini-3.0-pro'). Default: 'gemini-2.5-flash'. Example: gemini-2.5-flash.
            project_id (Union[Unset, str]): The Google Cloud project ID.
            location (Union[Unset, str]): The Google Cloud location (e.g., 'us-central1').
            temperature (Union[Unset, float]): Controls randomness in generation (0.0-2.0).
            max_tokens (Union[Unset, int]): Maximum number of tokens to generate.
            top_p (Union[Unset, float]): Nucleus sampling parameter.
            top_k (Union[Unset, int]): Top-k sampling parameter.
            api_key (Union[Unset, str]): The Google API key.
            url (Union[Unset, str]): The URL of the Google API endpoint.
    """

    model: str = "gemini-2.5-flash"
    project_id: Union[Unset, str] = UNSET
    location: Union[Unset, str] = UNSET
    temperature: Union[Unset, float] = UNSET
    max_tokens: Union[Unset, int] = UNSET
    top_p: Union[Unset, float] = UNSET
    top_k: Union[Unset, int] = UNSET
    api_key: Union[Unset, str] = UNSET
    url: Union[Unset, str] = UNSET
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
