from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="OpenAIEmbedderConfig")


@_attrs_define
class OpenAIEmbedderConfig:
    """Configuration for the OpenAI embedding provider.

    API key via `api_key` field or `OPENAI_API_KEY` environment variable.
    Supports OpenAI-compatible APIs via `url` field.

    **Example Models:** text-embedding-3-small (default, 1536 dims), text-embedding-3-large (3072 dims)

    **Docs:** https://platform.openai.com/docs/guides/embeddings

        Example:
            {'provider': 'openai', 'model': 'text-embedding-3-small', 'api_key': 'sk-...'}

        Attributes:
            model (str): The name of the OpenAI model to use. Default: 'text-embedding-3-small'. Example: text-
                embedding-3-small.
            url (Union[Unset, str]): The URL of the OpenAI API endpoint. Defaults to OpenAI's API. Can be set via
                OPENAI_BASE_URL environment variable. Default: 'https://api.openai.com'. Example: https://api.openai.com.
            api_key (Union[Unset, str]): The OpenAI API key. Can also be set via OPENAI_API_KEY environment variable.
            dimensions (Union[Unset, int]): Output dimension for the embedding (uses MRL for dimension reduction).
                Recommended: 256, 512, 1024, 1536, or 3072.
    """

    model: str = "text-embedding-3-small"
    url: Union[Unset, str] = "https://api.openai.com"
    api_key: Union[Unset, str] = UNSET
    dimensions: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        url = self.url

        api_key = self.api_key

        dimensions = self.dimensions

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if url is not UNSET:
            field_dict["url"] = url
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        url = d.pop("url", UNSET)

        api_key = d.pop("api_key", UNSET)

        dimensions = d.pop("dimensions", UNSET)

        open_ai_embedder_config = cls(
            model=model,
            url=url,
            api_key=api_key,
            dimensions=dimensions,
        )

        open_ai_embedder_config.additional_properties = d
        return open_ai_embedder_config

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
