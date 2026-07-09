from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_embed_response_object import InferenceEmbedResponseObject
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_embedding_batch_summary import InferenceEmbeddingBatchSummary
    from ..models.inference_embedding_item_error import InferenceEmbeddingItemError
    from ..models.inference_embedding_object import InferenceEmbeddingObject
    from ..models.inference_embedding_usage import InferenceEmbeddingUsage


T = TypeVar("T", bound="InferenceEmbedResponse")


@_attrs_define
class InferenceEmbedResponse:
    """OpenAI-compatible embedding response with a polymorphic `embedding` field for dense or sparse vectors

    Attributes:
        object_ (InferenceEmbedResponseObject): Object type, always "list"
        data (list[InferenceEmbeddingObject]): List of embedding objects
        model (str): Model used for embedding generation
        usage (InferenceEmbeddingUsage): Token usage information
        errors (list[InferenceEmbeddingItemError] | Unset): Indexed per-input failures. Only populated when request
            error_policy is per_item.
        summary (InferenceEmbeddingBatchSummary | Unset): Counts for per-item embedding responses
    """

    object_: InferenceEmbedResponseObject
    data: list[InferenceEmbeddingObject]
    model: str
    usage: InferenceEmbeddingUsage
    errors: list[InferenceEmbeddingItemError] | Unset = UNSET
    summary: InferenceEmbeddingBatchSummary | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        data = []
        for data_item_data in self.data:
            data_item = data_item_data.to_dict()
            data.append(data_item)

        model = self.model

        usage = self.usage.to_dict()

        errors: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.errors, Unset):
            errors = []
            for errors_item_data in self.errors:
                errors_item = errors_item_data.to_dict()
                errors.append(errors_item)

        summary: dict[str, Any] | Unset = UNSET
        if not isinstance(self.summary, Unset):
            summary = self.summary.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "data": data,
                "model": model,
                "usage": usage,
            }
        )
        if errors is not UNSET:
            field_dict["errors"] = errors
        if summary is not UNSET:
            field_dict["summary"] = summary

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_embedding_batch_summary import InferenceEmbeddingBatchSummary
        from ..models.inference_embedding_item_error import InferenceEmbeddingItemError
        from ..models.inference_embedding_object import InferenceEmbeddingObject
        from ..models.inference_embedding_usage import InferenceEmbeddingUsage

        d = dict(src_dict)
        object_ = InferenceEmbedResponseObject(d.pop("object"))

        data = []
        _data = d.pop("data")
        for data_item_data in _data:
            data_item = InferenceEmbeddingObject.from_dict(data_item_data)

            data.append(data_item)

        model = d.pop("model")

        usage = InferenceEmbeddingUsage.from_dict(d.pop("usage"))

        _errors = d.pop("errors", UNSET)
        errors: list[InferenceEmbeddingItemError] | Unset = UNSET
        if _errors is not UNSET:
            errors = []
            for errors_item_data in _errors:
                errors_item = InferenceEmbeddingItemError.from_dict(errors_item_data)

                errors.append(errors_item)

        _summary = d.pop("summary", UNSET)
        summary: InferenceEmbeddingBatchSummary | Unset
        if isinstance(_summary, Unset):
            summary = UNSET
        else:
            summary = InferenceEmbeddingBatchSummary.from_dict(_summary)

        inference_embed_response = cls(
            object_=object_,
            data=data,
            model=model,
            usage=usage,
            errors=errors,
            summary=summary,
        )

        inference_embed_response.additional_properties = d
        return inference_embed_response

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
