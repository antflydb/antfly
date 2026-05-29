from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_chunk_response_object import InferenceChunkResponseObject

if TYPE_CHECKING:
    from ..models.inference_chunk_object import InferenceChunkObject
    from ..models.inference_generate_usage import InferenceGenerateUsage


T = TypeVar("T", bound="InferenceChunkResponse")


@_attrs_define
class InferenceChunkResponse:
    """
    Example:
        {'object': 'list', 'data': [{'object': 'chunk', 'index': 0, 'id': 0, 'text': 'This is the first chunk...',
            'start_char': 0, 'end_char': 100, 'mime_type': 'text/plain'}, {'object': 'chunk', 'index': 1, 'id': 1, 'text':
            'This is the second chunk...', 'start_char': 90, 'end_char': 190, 'mime_type': 'text/plain'}], 'model': 'fixed',
            'usage': {'prompt_tokens': 12, 'completion_tokens': 0, 'total_tokens': 12}, 'cache_hit': False}

    Attributes:
        object_ (InferenceChunkResponseObject): Object type, always "list"
        data (list[InferenceChunkObject]): Array of chunk objects
        model (str): Chunking model actually used (may differ from requested if fallback occurred) Example: fixed.
        usage (InferenceGenerateUsage):
        cache_hit (bool): Whether result was served from cache
    """

    object_: InferenceChunkResponseObject
    data: list[InferenceChunkObject]
    model: str
    usage: InferenceGenerateUsage
    cache_hit: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        data = []
        for data_item_data in self.data:
            data_item = data_item_data.to_dict()
            data.append(data_item)

        model = self.model

        usage = self.usage.to_dict()

        cache_hit = self.cache_hit

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "data": data,
                "model": model,
                "usage": usage,
                "cache_hit": cache_hit,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_chunk_object import InferenceChunkObject
        from ..models.inference_generate_usage import InferenceGenerateUsage

        d = dict(src_dict)
        object_ = InferenceChunkResponseObject(d.pop("object"))

        data = []
        _data = d.pop("data")
        for data_item_data in _data:
            data_item = InferenceChunkObject.from_dict(data_item_data)

            data.append(data_item)

        model = d.pop("model")

        usage = InferenceGenerateUsage.from_dict(d.pop("usage"))

        cache_hit = d.pop("cache_hit")

        inference_chunk_response = cls(
            object_=object_,
            data=data,
            model=model,
            usage=usage,
            cache_hit=cache_hit,
        )

        inference_chunk_response.additional_properties = d
        return inference_chunk_response

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
