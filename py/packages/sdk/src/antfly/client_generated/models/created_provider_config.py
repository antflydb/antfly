from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="CreatedProviderConfig")


@_attrs_define
class CreatedProviderConfig:
    """Credential-free provider configuration returned after index creation. Only non-secret provider settings are
    represented.

        Attributes:
            provider (str): Configured provider discriminator.
            model (str | Unset): Configured provider model when applicable.
            models (list[str] | Unset):
            project_id (str | Unset):
            location (str | Unset):
            region (str | Unset):
            url (str | Unset):
            api_url (str | Unset):
            dimension (int | Unset):
            dimensions (int | Unset):
            input_type (str | Unset):
            truncate (str | Unset):
            strip_new_lines (bool | Unset):
            batch_size (int | Unset):
            temperature (float | Unset):
            max_tokens (int | Unset):
            top_p (float | Unset):
            top_k (int | Unset):
            frequency_penalty (float | Unset):
            presence_penalty (float | Unset):
            timeout (int | Unset):
    """

    provider: str
    model: str | Unset = UNSET
    models: list[str] | Unset = UNSET
    project_id: str | Unset = UNSET
    location: str | Unset = UNSET
    region: str | Unset = UNSET
    url: str | Unset = UNSET
    api_url: str | Unset = UNSET
    dimension: int | Unset = UNSET
    dimensions: int | Unset = UNSET
    input_type: str | Unset = UNSET
    truncate: str | Unset = UNSET
    strip_new_lines: bool | Unset = UNSET
    batch_size: int | Unset = UNSET
    temperature: float | Unset = UNSET
    max_tokens: int | Unset = UNSET
    top_p: float | Unset = UNSET
    top_k: int | Unset = UNSET
    frequency_penalty: float | Unset = UNSET
    presence_penalty: float | Unset = UNSET
    timeout: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider

        model = self.model

        models: list[str] | Unset = UNSET
        if not isinstance(self.models, Unset):
            models = self.models

        project_id = self.project_id

        location = self.location

        region = self.region

        url = self.url

        api_url = self.api_url

        dimension = self.dimension

        dimensions = self.dimensions

        input_type = self.input_type

        truncate = self.truncate

        strip_new_lines = self.strip_new_lines

        batch_size = self.batch_size

        temperature = self.temperature

        max_tokens = self.max_tokens

        top_p = self.top_p

        top_k = self.top_k

        frequency_penalty = self.frequency_penalty

        presence_penalty = self.presence_penalty

        timeout = self.timeout

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "provider": provider,
            }
        )
        if model is not UNSET:
            field_dict["model"] = model
        if models is not UNSET:
            field_dict["models"] = models
        if project_id is not UNSET:
            field_dict["project_id"] = project_id
        if location is not UNSET:
            field_dict["location"] = location
        if region is not UNSET:
            field_dict["region"] = region
        if url is not UNSET:
            field_dict["url"] = url
        if api_url is not UNSET:
            field_dict["api_url"] = api_url
        if dimension is not UNSET:
            field_dict["dimension"] = dimension
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions
        if input_type is not UNSET:
            field_dict["input_type"] = input_type
        if truncate is not UNSET:
            field_dict["truncate"] = truncate
        if strip_new_lines is not UNSET:
            field_dict["strip_new_lines"] = strip_new_lines
        if batch_size is not UNSET:
            field_dict["batch_size"] = batch_size
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
        if timeout is not UNSET:
            field_dict["timeout"] = timeout

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = d.pop("provider")

        model = d.pop("model", UNSET)

        models = cast(list[str], d.pop("models", UNSET))

        project_id = d.pop("project_id", UNSET)

        location = d.pop("location", UNSET)

        region = d.pop("region", UNSET)

        url = d.pop("url", UNSET)

        api_url = d.pop("api_url", UNSET)

        dimension = d.pop("dimension", UNSET)

        dimensions = d.pop("dimensions", UNSET)

        input_type = d.pop("input_type", UNSET)

        truncate = d.pop("truncate", UNSET)

        strip_new_lines = d.pop("strip_new_lines", UNSET)

        batch_size = d.pop("batch_size", UNSET)

        temperature = d.pop("temperature", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        top_p = d.pop("top_p", UNSET)

        top_k = d.pop("top_k", UNSET)

        frequency_penalty = d.pop("frequency_penalty", UNSET)

        presence_penalty = d.pop("presence_penalty", UNSET)

        timeout = d.pop("timeout", UNSET)

        created_provider_config = cls(
            provider=provider,
            model=model,
            models=models,
            project_id=project_id,
            location=location,
            region=region,
            url=url,
            api_url=api_url,
            dimension=dimension,
            dimensions=dimensions,
            input_type=input_type,
            truncate=truncate,
            strip_new_lines=strip_new_lines,
            batch_size=batch_size,
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=top_p,
            top_k=top_k,
            frequency_penalty=frequency_penalty,
            presence_penalty=presence_penalty,
            timeout=timeout,
        )

        return created_provider_config
