from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_embedding_object_object import InferenceEmbeddingObjectObject
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_sparse_vector import InferenceSparseVector


T = TypeVar("T", bound="InferenceEmbeddingObject")


@_attrs_define
class InferenceEmbeddingObject:
    """A single embedding result

    Attributes:
        object_ (InferenceEmbeddingObjectObject): Object type, always "embedding"
        index (int): Index of the input this embedding corresponds to
        embedding (InferenceSparseVector | list[float] | Unset): Dense float vector for dense models, or a sparse vector
            object for sparse-capable models
    """

    object_: InferenceEmbeddingObjectObject
    index: int
    embedding: InferenceSparseVector | list[float] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        index = self.index

        embedding: dict[str, Any] | list[float] | Unset
        if isinstance(self.embedding, Unset):
            embedding = UNSET
        elif isinstance(self.embedding, list):
            embedding = self.embedding

        else:
            embedding = self.embedding.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "index": index,
            }
        )
        if embedding is not UNSET:
            field_dict["embedding"] = embedding

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_sparse_vector import InferenceSparseVector

        d = dict(src_dict)
        object_ = InferenceEmbeddingObjectObject(d.pop("object"))

        index = d.pop("index")

        def _parse_embedding(data: object) -> InferenceSparseVector | list[float] | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                embedding_type_0 = cast(list[float], data)

                return embedding_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            embedding_type_1 = InferenceSparseVector.from_dict(data)

            return embedding_type_1

        embedding = _parse_embedding(d.pop("embedding", UNSET))

        inference_embedding_object = cls(
            object_=object_,
            index=index,
            embedding=embedding,
        )

        inference_embedding_object.additional_properties = d
        return inference_embedding_object

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
