from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.cloudflare_stt_config_provider import CloudflareSTTConfigProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="CloudflareSTTConfig")


@_attrs_define
class CloudflareSTTConfig:
    """Cloudflare Workers AI native speech adapter. Credentials use CLOUDFLARE_API_TOKEN.

    Attributes:
        provider (CloudflareSTTConfigProvider):
        account_id (str):
        model (str):  Default: '@cf/openai/whisper-large-v3-turbo'.
        gateway_id (str | Unset):
        api_key (str | Unset):
        base_url (str | Unset): Optional deployment-owned full native endpoint override.
        language_code (str | Unset):
        voice (str | Unset):
    """

    provider: CloudflareSTTConfigProvider
    account_id: str
    model: str = "@cf/openai/whisper-large-v3-turbo"
    gateway_id: str | Unset = UNSET
    api_key: str | Unset = UNSET
    base_url: str | Unset = UNSET
    language_code: str | Unset = UNSET
    voice: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        account_id = self.account_id

        model = self.model

        gateway_id = self.gateway_id

        api_key = self.api_key

        base_url = self.base_url

        language_code = self.language_code

        voice = self.voice

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
        if base_url is not UNSET:
            field_dict["base_url"] = base_url
        if language_code is not UNSET:
            field_dict["language_code"] = language_code
        if voice is not UNSET:
            field_dict["voice"] = voice

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = CloudflareSTTConfigProvider(d.pop("provider"))

        account_id = d.pop("account_id")

        model = d.pop("model")

        gateway_id = d.pop("gateway_id", UNSET)

        api_key = d.pop("api_key", UNSET)

        base_url = d.pop("base_url", UNSET)

        language_code = d.pop("language_code", UNSET)

        voice = d.pop("voice", UNSET)

        cloudflare_stt_config = cls(
            provider=provider,
            account_id=account_id,
            model=model,
            gateway_id=gateway_id,
            api_key=api_key,
            base_url=base_url,
            language_code=language_code,
            voice=voice,
        )

        cloudflare_stt_config.additional_properties = d
        return cloudflare_stt_config

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
