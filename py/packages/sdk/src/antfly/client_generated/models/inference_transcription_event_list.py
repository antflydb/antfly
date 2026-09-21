from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_transcription_event_list_object import InferenceTranscriptionEventListObject

if TYPE_CHECKING:
    from ..models.inference_transcription_event import InferenceTranscriptionEvent


T = TypeVar("T", bound="InferenceTranscriptionEventList")


@_attrs_define
class InferenceTranscriptionEventList:
    """
    Attributes:
        object_ (InferenceTranscriptionEventListObject):
        session_id (str):
        model (str):
        data (list[InferenceTranscriptionEvent]):
        buffered_ms (int):
        total_ms (int):
    """

    object_: InferenceTranscriptionEventListObject
    session_id: str
    model: str
    data: list[InferenceTranscriptionEvent]
    buffered_ms: int
    total_ms: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        session_id = self.session_id

        model = self.model

        data = []
        for data_item_data in self.data:
            data_item = data_item_data.to_dict()
            data.append(data_item)

        buffered_ms = self.buffered_ms

        total_ms = self.total_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "session_id": session_id,
                "model": model,
                "data": data,
                "buffered_ms": buffered_ms,
                "total_ms": total_ms,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_transcription_event import InferenceTranscriptionEvent

        d = dict(src_dict)
        object_ = InferenceTranscriptionEventListObject(d.pop("object"))

        session_id = d.pop("session_id")

        model = d.pop("model")

        data = []
        _data = d.pop("data")
        for data_item_data in _data:
            data_item = InferenceTranscriptionEvent.from_dict(data_item_data)

            data.append(data_item)

        buffered_ms = d.pop("buffered_ms")

        total_ms = d.pop("total_ms")

        inference_transcription_event_list = cls(
            object_=object_,
            session_id=session_id,
            model=model,
            data=data,
            buffered_ms=buffered_ms,
            total_ms=total_ms,
        )

        inference_transcription_event_list.additional_properties = d
        return inference_transcription_event_list

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
