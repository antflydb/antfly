from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.cloudflare_embedder_config_provider import CloudflareEmbedderConfigProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="CloudflareEmbedderConfig")


@_attrs_define
class CloudflareEmbedderConfig:
    """Cloudflare Workers AI embedding. Uses CLOUDFLARE_API_TOKEN or an Antfly secret reference.

    Attributes:
        provider (CloudflareEmbedderConfigProvider):
        model (str):  Default: '@cf/baai/bge-base-en-v1.5'.
        account_id (str | Unset): Required for a direct model. A connection-qualified model takes the account from its
            connection.
        gateway_id (str | Unset): Optional AI Gateway ID, sent through cf-aig-gateway-id.
        api_key (str | Unset):
        url (str | Unset): Optional deployment-owned endpoint override.
        dimensions (int | Unset): Expected fixed model output dimension; the adapter validates returned vectors and
            never requests unsupported resizing.
        dimension (int | Unset): Alias of dimensions.
    """

    provider: CloudflareEmbedderConfigProvider
    model: str = "@cf/baai/bge-base-en-v1.5"
    account_id: str | Unset = UNSET
    gateway_id: str | Unset = UNSET
    api_key: str | Unset = UNSET
    url: str | Unset = UNSET
    dimensions: int | Unset = UNSET
    dimension: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        model = self.model

        account_id = self.account_id

        gateway_id = self.gateway_id

        api_key = self.api_key

        url = self.url

        dimensions = self.dimensions

        dimension = self.dimension

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
                "model": model,
            }
        )
        if account_id is not UNSET:
            field_dict["account_id"] = account_id
        if gateway_id is not UNSET:
            field_dict["gateway_id"] = gateway_id
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if url is not UNSET:
            field_dict["url"] = url
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions
        if dimension is not UNSET:
            field_dict["dimension"] = dimension

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = CloudflareEmbedderConfigProvider(d.pop("provider"))

        model = d.pop("model")

        account_id = d.pop("account_id", UNSET)

        gateway_id = d.pop("gateway_id", UNSET)

        api_key = d.pop("api_key", UNSET)

        url = d.pop("url", UNSET)

        dimensions = d.pop("dimensions", UNSET)

        dimension = d.pop("dimension", UNSET)

        cloudflare_embedder_config = cls(
            provider=provider,
            model=model,
            account_id=account_id,
            gateway_id=gateway_id,
            api_key=api_key,
            url=url,
            dimensions=dimensions,
            dimension=dimension,
        )

        cloudflare_embedder_config.additional_properties = d
        return cloudflare_embedder_config

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
