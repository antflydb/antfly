from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="VertexGeneratorConfig")


@_attrs_define
class VertexGeneratorConfig:
    """Configuration for Google Cloud Vertex AI generative models.

    Attributes:
        model (str): The name of the Vertex AI model to use. Default: 'gemini-2.5-flash'. Example: gemini-2.5-flash.
        project_id (str | Unset): Google Cloud project ID.
        location (str | Unset): Google Cloud region for Vertex AI API. Default: 'us-central1'.
        credentials_path (str | Unset): Path to service account JSON key file.
        temperature (float | Unset): Controls randomness in generation (0.0-2.0).
        max_tokens (int | Unset): Maximum number of tokens to generate in the response.
        top_p (float | Unset): Nucleus sampling parameter (0.0-1.0).
        top_k (int | Unset): Top-k sampling parameter.
    """

    model: str = "gemini-2.5-flash"
    project_id: str | Unset = UNSET
    location: str | Unset = "us-central1"
    credentials_path: str | Unset = UNSET
    temperature: float | Unset = UNSET
    max_tokens: int | Unset = UNSET
    top_p: float | Unset = UNSET
    top_k: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        project_id = self.project_id

        location = self.location

        credentials_path = self.credentials_path

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
        if project_id is not UNSET:
            field_dict["project_id"] = project_id
        if location is not UNSET:
            field_dict["location"] = location
        if credentials_path is not UNSET:
            field_dict["credentials_path"] = credentials_path
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

        project_id = d.pop("project_id", UNSET)

        location = d.pop("location", UNSET)

        credentials_path = d.pop("credentials_path", UNSET)

        temperature = d.pop("temperature", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        top_p = d.pop("top_p", UNSET)

        top_k = d.pop("top_k", UNSET)

        vertex_generator_config = cls(
            model=model,
            project_id=project_id,
            location=location,
            credentials_path=credentials_path,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            top_k=top_k,
        )

        vertex_generator_config.additional_properties = d
        return vertex_generator_config

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
