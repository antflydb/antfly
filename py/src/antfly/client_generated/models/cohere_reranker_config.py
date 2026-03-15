from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

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
            model (str): The name of the Cohere reranking model to use. Default: 'rerank-english-v3.0'. Example: rerank-
                english-v3.0.
            api_key (Union[Unset, str]): The Cohere API key. Can also be set via COHERE_API_KEY environment variable.
            top_n (Union[Unset, int]): Number of most relevant documents to return. If not specified, returns all documents
                with scores.
            max_chunks_per_doc (Union[Unset, int]): Maximum number of chunks per document for long document handling.
    """

    model: str = "rerank-english-v3.0"
    api_key: Union[Unset, str] = UNSET
    top_n: Union[Unset, int] = UNSET
    max_chunks_per_doc: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        api_key = self.api_key

        top_n = self.top_n

        max_chunks_per_doc = self.max_chunks_per_doc

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if top_n is not UNSET:
            field_dict["top_n"] = top_n
        if max_chunks_per_doc is not UNSET:
            field_dict["max_chunks_per_doc"] = max_chunks_per_doc

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        api_key = d.pop("api_key", UNSET)

        top_n = d.pop("top_n", UNSET)

        max_chunks_per_doc = d.pop("max_chunks_per_doc", UNSET)

        cohere_reranker_config = cls(
            model=model,
            api_key=api_key,
            top_n=top_n,
            max_chunks_per_doc=max_chunks_per_doc,
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
