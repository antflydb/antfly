from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_embed_response_object import InferenceEmbedResponseObject

if TYPE_CHECKING:
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
    """

    object_: InferenceEmbedResponseObject
    data: list[InferenceEmbeddingObject]
    model: str
    usage: InferenceEmbeddingUsage
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        data = []
        for data_item_data in self.data:
            data_item = data_item_data.to_dict()
            data.append(data_item)

        model = self.model

        usage = self.usage.to_dict()

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

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
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

        inference_embed_response = cls(
            object_=object_,
            data=data,
            model=model,
            usage=usage,
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
