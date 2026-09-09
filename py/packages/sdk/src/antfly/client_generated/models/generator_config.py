from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.generator_provider import GeneratorProvider
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rate_limit_config import RateLimitConfig


T = TypeVar("T", bound="GeneratorConfig")


@_attrs_define
class GeneratorConfig:
    """A unified configuration for a generative AI provider.

    Example:
        {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}

    Attributes:
        provider (GeneratorProvider): Generator providers implemented by Antfly's generation runtime.
        rate_limit (RateLimitConfig | Unset): Outbound provider limits shared within one Antfly process by effective
            endpoint, operation, model, credential source, project and region/location.
            Conflicting policies for an active scope are rejected. These limits do
            not coordinate across replicas or infer the provider's account quota.
    """

    provider: GeneratorProvider
    rate_limit: RateLimitConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        rate_limit: dict[str, Any] | Unset = UNSET
        if not isinstance(self.rate_limit, Unset):
            rate_limit = self.rate_limit.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
            }
        )
        if rate_limit is not UNSET:
            field_dict["rate_limit"] = rate_limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rate_limit_config import RateLimitConfig

        d = dict(src_dict)
        provider = GeneratorProvider(d.pop("provider"))

        _rate_limit = d.pop("rate_limit", UNSET)
        rate_limit: RateLimitConfig | Unset
        if isinstance(_rate_limit, Unset):
            rate_limit = UNSET
        else:
            rate_limit = RateLimitConfig.from_dict(_rate_limit)

        generator_config = cls(
            provider=provider,
            rate_limit=rate_limit,
        )

        generator_config.additional_properties = d
        return generator_config

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
