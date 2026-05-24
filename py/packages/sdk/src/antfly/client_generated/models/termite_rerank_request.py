from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="TermiteRerankRequest")


@_attrs_define
class TermiteRerankRequest:
    """
    Attributes:
        model (str): Name of reranking model from models_dir/rerankers/ Example: BAAI/bge-reranker-v2-m3.
        query (str): Search query for relevance scoring Example: machine learning applications.
        prompts (list[str]): Pre-rendered document texts to rerank. The client is responsible for extracting
            and rendering document fields/templates before calling this endpoint.
             Example: ['Introduction to machine learning...', 'Deep learning fundamentals...'].
    """

    model: str
    query: str
    prompts: list[str]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        query = self.query

        prompts = self.prompts

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "query": query,
                "prompts": prompts,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        query = d.pop("query")

        prompts = cast(list[str], d.pop("prompts"))

        termite_rerank_request = cls(
            model=model,
            query=query,
            prompts=prompts,
        )

        termite_rerank_request.additional_properties = d
        return termite_rerank_request

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
