from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.apple_generator_config_provider import AppleGeneratorConfigProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="AppleGeneratorConfig")


@_attrs_define
class AppleGeneratorConfig:
    """On-device Apple Foundation Models generation. Requires a macOS Apple provider build, Apple Intelligence enabled, and
    its system model ready. Supports text conversations; tool calling and media attachments are not supported.

        Attributes:
            provider (AppleGeneratorConfigProvider | Unset):
            max_tokens (int | Unset):  Default: 256.
            temperature (float | Unset):
    """

    provider: AppleGeneratorConfigProvider | Unset = UNSET
    max_tokens: int | Unset = 256
    temperature: float | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider: str | Unset = UNSET
        if not isinstance(self.provider, Unset):
            provider = self.provider.value

        max_tokens = self.max_tokens

        temperature = self.temperature

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if provider is not UNSET:
            field_dict["provider"] = provider
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens
        if temperature is not UNSET:
            field_dict["temperature"] = temperature

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _provider = d.pop("provider", UNSET)
        provider: AppleGeneratorConfigProvider | Unset
        if isinstance(_provider, Unset):
            provider = UNSET
        else:
            provider = AppleGeneratorConfigProvider(_provider)

        max_tokens = d.pop("max_tokens", UNSET)

        temperature = d.pop("temperature", UNSET)

        apple_generator_config = cls(
            provider=provider,
            max_tokens=max_tokens,
            temperature=temperature,
        )

        apple_generator_config.additional_properties = d
        return apple_generator_config

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
