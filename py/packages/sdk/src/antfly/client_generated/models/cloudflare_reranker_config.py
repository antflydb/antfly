from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.cloudflare_reranker_config_provider import CloudflareRerankerConfigProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="CloudflareRerankerConfig")


@_attrs_define
class CloudflareRerankerConfig:
    """Cloudflare Workers AI reranking. Uses CLOUDFLARE_API_TOKEN or an Antfly secret reference.

    Attributes:
        provider (CloudflareRerankerConfigProvider):
        account_id (str):
        model (str):  Default: '@cf/baai/bge-reranker-base'.
        gateway_id (str | Unset): Optional AI Gateway ID, sent through cf-aig-gateway-id.
        api_key (str | Unset):
        url (str | Unset): Optional deployment-owned endpoint override.
    """

    provider: CloudflareRerankerConfigProvider
    account_id: str
    model: str = "@cf/baai/bge-reranker-base"
    gateway_id: str | Unset = UNSET
    api_key: str | Unset = UNSET
    url: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        account_id = self.account_id

        model = self.model

        gateway_id = self.gateway_id

        api_key = self.api_key

        url = self.url

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

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = CloudflareRerankerConfigProvider(d.pop("provider"))

        account_id = d.pop("account_id")

        model = d.pop("model")

        gateway_id = d.pop("gateway_id", UNSET)

        api_key = d.pop("api_key", UNSET)

        url = d.pop("url", UNSET)

        cloudflare_reranker_config = cls(
            provider=provider,
            account_id=account_id,
            model=model,
            gateway_id=gateway_id,
            api_key=api_key,
            url=url,
        )

        cloudflare_reranker_config.additional_properties = d
        return cloudflare_reranker_config

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
