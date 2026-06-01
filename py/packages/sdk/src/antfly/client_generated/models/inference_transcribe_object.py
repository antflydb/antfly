from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_transcribe_object_object import InferenceTranscribeObjectObject
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceTranscribeObject")


@_attrs_define
class InferenceTranscribeObject:
    """
    Attributes:
        object_ (InferenceTranscribeObjectObject):
        index (int): Input audio index.
        text (str): Transcribed text from the audio Example: Hello, how are you today?.
        language (str | Unset): Detected or forced language Example: en.
    """

    object_: InferenceTranscribeObjectObject
    index: int
    text: str
    language: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        index = self.index

        text = self.text

        language = self.language

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "index": index,
                "text": text,
            }
        )
        if language is not UNSET:
            field_dict["language"] = language

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        object_ = InferenceTranscribeObjectObject(d.pop("object"))

        index = d.pop("index")

        text = d.pop("text")

        language = d.pop("language", UNSET)

        inference_transcribe_object = cls(
            object_=object_,
            index=index,
            text=text,
            language=language,
        )

        inference_transcribe_object.additional_properties = d
        return inference_transcribe_object

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
