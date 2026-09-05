from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="EmbeddingRetrievalConfig")


@_attrs_define
class EmbeddingRetrievalConfig:
    """Advanced retrieval-role overrides. Antfly assigns canonical task intent
    automatically: semantic-search inputs are `RETRIEVAL_QUERY`, while index
    and artifact writes are `RETRIEVAL_DOCUMENT`. These fields only override
    how a provider or instruction-aware model represents that intent.

        Attributes:
            query_input_type (str | Unset): Provider-specific query role, such as `search_query` for Cohere.
                When omitted, the provider adapter derives it from
                `RETRIEVAL_QUERY`.
            document_input_type (str | Unset): Provider-specific document role, such as `search_document` for
                Cohere. When omitted, the provider adapter derives it from
                `RETRIEVAL_DOCUMENT`.
            query_instruction (str | Unset): Optional instruction sent only with retrieval-query embeddings by
                instruction-aware Antfly inference models. Provider adapters that
                do not support free-form instructions reject this field.
    """

    query_input_type: str | Unset = UNSET
    document_input_type: str | Unset = UNSET
    query_instruction: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        query_input_type = self.query_input_type

        document_input_type = self.document_input_type

        query_instruction = self.query_instruction

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if query_input_type is not UNSET:
            field_dict["query_input_type"] = query_input_type
        if document_input_type is not UNSET:
            field_dict["document_input_type"] = document_input_type
        if query_instruction is not UNSET:
            field_dict["query_instruction"] = query_instruction

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        query_input_type = d.pop("query_input_type", UNSET)

        document_input_type = d.pop("document_input_type", UNSET)

        query_instruction = d.pop("query_instruction", UNSET)

        embedding_retrieval_config = cls(
            query_input_type=query_input_type,
            document_input_type=document_input_type,
            query_instruction=query_instruction,
        )

        return embedding_retrieval_config
