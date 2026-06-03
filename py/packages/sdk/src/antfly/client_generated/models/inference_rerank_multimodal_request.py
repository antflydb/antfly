from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.inference_rerank_multimodal_document import InferenceRerankMultimodalDocument


T = TypeVar("T", bound="InferenceRerankMultimodalRequest")


@_attrs_define
class InferenceRerankMultimodalRequest:
    """
    Attributes:
        model (str): Name of multimodal reranking model from models_dir/rerankers/ Example: vidore/colqwen2-v1.0.
        query (str): Text query for relevance scoring Example: invoice total due date.
        documents (list[InferenceRerankMultimodalDocument]): Documents expressed as text and image content parts
    """

    model: str
    query: str
    documents: list[InferenceRerankMultimodalDocument]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        query = self.query

        documents = []
        for documents_item_data in self.documents:
            documents_item = documents_item_data.to_dict()
            documents.append(documents_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "query": query,
                "documents": documents,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_rerank_multimodal_document import InferenceRerankMultimodalDocument

        d = dict(src_dict)
        model = d.pop("model")

        query = d.pop("query")

        documents = []
        _documents = d.pop("documents")
        for documents_item_data in _documents:
            documents_item = InferenceRerankMultimodalDocument.from_dict(documents_item_data)

            documents.append(documents_item)

        inference_rerank_multimodal_request = cls(
            model=model,
            query=query,
            documents=documents,
        )

        inference_rerank_multimodal_request.additional_properties = d
        return inference_rerank_multimodal_request

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
