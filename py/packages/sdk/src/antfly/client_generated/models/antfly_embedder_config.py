from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AntflyEmbedderConfig")


@_attrs_define
class AntflyEmbedderConfig:
    """Configuration for the Antfly inference embedding provider.

    Antfly inference is Antfly's built-in ML service for local embeddings using ONNX models.
    It provides embedding generation with multi-tier caching (memory + persistent).

    **Features:**
    - Local ONNX-based embedding generation
    - L1 memory cache with configurable TTL
    - L2 persistent Pebble database cache
    - Singleflight deduplication for concurrent identical requests

    **Example Models:** bge-base-en-v1.5 (768 dims), all-MiniLM-L6-v2 (384 dims)

    Models are loaded from the `models/embedders/{name}/` directory.

        Example:
            {'provider': 'antfly', 'model': 'bge-base-en-v1.5', 'api_url': 'http://localhost:8082'}

        Attributes:
            model (str): The embedding model name (maps to models/embedders/{name}/ directory). Example: bge-base-en-v1.5.
            api_url (str | Unset): The URL of the Inference API endpoint. Can also be set via ANTFLY_INFERENCE_URL
                environment variable. Example: http://localhost:8082.
    """

    model: str
    api_url: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        api_url = self.api_url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if api_url is not UNSET:
            field_dict["api_url"] = api_url

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        api_url = d.pop("api_url", UNSET)

        antfly_embedder_config = cls(
            model=model,
            api_url=api_url,
        )

        antfly_embedder_config.additional_properties = d
        return antfly_embedder_config

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
