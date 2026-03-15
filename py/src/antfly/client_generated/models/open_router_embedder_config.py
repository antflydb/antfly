from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="OpenRouterEmbedderConfig")


@_attrs_define
class OpenRouterEmbedderConfig:
    """Configuration for the OpenRouter embedding provider.

    OpenRouter provides a unified API for multiple embedding models from different providers.
    API key via `api_key` field or `OPENROUTER_API_KEY` environment variable.

    **Example Models:** openai/text-embedding-3-small (default), openai/text-embedding-3-large,
    google/gemini-embedding-001, qwen/qwen3-embedding-8b

    **Docs:** https://openrouter.ai/docs/api/reference/embeddings

        Example:
            {'provider': 'openrouter', 'model': 'openai/text-embedding-3-small', 'api_key': 'sk-or-...'}

        Attributes:
            model (str): The OpenRouter model identifier (e.g., 'openai/text-embedding-3-small', 'google/gemini-
                embedding-001'). Default: 'openai/text-embedding-3-small'. Example: openai/text-embedding-3-small.
            api_key (Union[Unset, str]): The OpenRouter API key. Can also be set via OPENROUTER_API_KEY environment
                variable.
            dimensions (Union[Unset, int]): Output dimension for the embedding (if supported by the model).
    """

    model: str = "openai/text-embedding-3-small"
    api_key: Union[Unset, str] = UNSET
    dimensions: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        api_key = self.api_key

        dimensions = self.dimensions

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        api_key = d.pop("api_key", UNSET)

        dimensions = d.pop("dimensions", UNSET)

        open_router_embedder_config = cls(
            model=model,
            api_key=api_key,
            dimensions=dimensions,
        )

        open_router_embedder_config.additional_properties = d
        return open_router_embedder_config

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
