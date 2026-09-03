from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.cohere_reranker_config_provider import CohereRerankerConfigProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="CohereRerankerConfig")


@_attrs_define
class CohereRerankerConfig:
    """Configuration for the Cohere reranking provider.

    API key via `api_key` field or `COHERE_API_KEY` environment variable.

    **Example Models:** rerank-english-v3.0 (default), rerank-multilingual-v3.0

    **Docs:** https://docs.cohere.com/reference/rerank

        Example:
            {'provider': 'cohere', 'model': 'rerank-english-v3.0'}

        Attributes:
            provider (CohereRerankerConfigProvider):
            model (str): The name of the Cohere reranking model to use. Default: 'rerank-english-v3.0'. Example: rerank-
                english-v3.0.
            api_key (str | Unset): The Cohere API key. Can also be set via COHERE_API_KEY environment variable.
    """

    provider: CohereRerankerConfigProvider
    model: str = "rerank-english-v3.0"
    api_key: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        model = self.model

        api_key = self.api_key

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
                "model": model,
            }
        )
        if api_key is not UNSET:
            field_dict["api_key"] = api_key

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = CohereRerankerConfigProvider(d.pop("provider"))

        model = d.pop("model")

        api_key = d.pop("api_key", UNSET)

        cohere_reranker_config = cls(
            provider=provider,
            model=model,
            api_key=api_key,
        )

        cohere_reranker_config.additional_properties = d
        return cohere_reranker_config

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
