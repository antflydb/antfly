from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AntflyRerankerConfig")


@_attrs_define
class AntflyRerankerConfig:
    """Configuration for the Antfly inference reranking provider.

    Example:
        {'provider': 'antfly', 'model': 'mixedbread-ai/mxbai-rerank-base-v1', 'url': 'http://localhost:8080'}

    Attributes:
        model (str): The name of the reranking model (e.g., cross-encoder model name).
        url (str | Unset): The URL of the Inference API endpoint. Can also be set via ANTFLY_INFERENCE_URL environment
            variable.
    """

    model: str
    url: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        url = self.url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if url is not UNSET:
            field_dict["url"] = url

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        url = d.pop("url", UNSET)

        antfly_reranker_config = cls(
            model=model,
            url=url,
        )

        antfly_reranker_config.additional_properties = d
        return antfly_reranker_config

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
