from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.cloudflare_generator_config_provider import CloudflareGeneratorConfigProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="CloudflareGeneratorConfig")


@_attrs_define
class CloudflareGeneratorConfig:
    """Cloudflare Workers AI generation. Uses CLOUDFLARE_API_TOKEN or an Antfly secret reference.

    Attributes:
        provider (CloudflareGeneratorConfigProvider):
        account_id (str):
        model (str):  Default: '@cf/meta/llama-3.3-70b-instruct-fp8-fast'.
        gateway_id (str | Unset): Optional AI Gateway ID, sent through cf-aig-gateway-id.
        api_key (str | Unset):
        url (str | Unset): Optional deployment-owned endpoint override.
        max_tokens (int | Unset):
        temperature (float | Unset):
        top_p (float | Unset):
        frequency_penalty (float | Unset):
        presence_penalty (float | Unset):
    """

    provider: CloudflareGeneratorConfigProvider
    account_id: str
    model: str = "@cf/meta/llama-3.3-70b-instruct-fp8-fast"
    gateway_id: str | Unset = UNSET
    api_key: str | Unset = UNSET
    url: str | Unset = UNSET
    max_tokens: int | Unset = UNSET
    temperature: float | Unset = UNSET
    top_p: float | Unset = UNSET
    frequency_penalty: float | Unset = UNSET
    presence_penalty: float | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        account_id = self.account_id

        model = self.model

        gateway_id = self.gateway_id

        api_key = self.api_key

        url = self.url

        max_tokens = self.max_tokens

        temperature = self.temperature

        top_p = self.top_p

        frequency_penalty = self.frequency_penalty

        presence_penalty = self.presence_penalty

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
                "account_id": account_id,
                "model": model,
            }
        )
        if gateway_id is not UNSET:
            field_dict["gateway_id"] = gateway_id
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if url is not UNSET:
            field_dict["url"] = url
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens
        if temperature is not UNSET:
            field_dict["temperature"] = temperature
        if top_p is not UNSET:
            field_dict["top_p"] = top_p
        if frequency_penalty is not UNSET:
            field_dict["frequency_penalty"] = frequency_penalty
        if presence_penalty is not UNSET:
            field_dict["presence_penalty"] = presence_penalty

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = CloudflareGeneratorConfigProvider(d.pop("provider"))

        account_id = d.pop("account_id")

        model = d.pop("model")

        gateway_id = d.pop("gateway_id", UNSET)

        api_key = d.pop("api_key", UNSET)

        url = d.pop("url", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        temperature = d.pop("temperature", UNSET)

        top_p = d.pop("top_p", UNSET)

        frequency_penalty = d.pop("frequency_penalty", UNSET)

        presence_penalty = d.pop("presence_penalty", UNSET)

        cloudflare_generator_config = cls(
            provider=provider,
            account_id=account_id,
            model=model,
            gateway_id=gateway_id,
            api_key=api_key,
            url=url,
            max_tokens=max_tokens,
            temperature=temperature,
            top_p=top_p,
            frequency_penalty=frequency_penalty,
            presence_penalty=presence_penalty,
        )

        cloudflare_generator_config.additional_properties = d
        return cloudflare_generator_config

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
