from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_transcription_event_object import InferenceTranscriptionEventObject
from ..models.inference_transcription_event_type import InferenceTranscriptionEventType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_dictation_word import InferenceDictationWord


T = TypeVar("T", bound="InferenceTranscriptionEvent")


@_attrs_define
class InferenceTranscriptionEvent:
    """
    Attributes:
        object_ (InferenceTranscriptionEventObject):
        type_ (InferenceTranscriptionEventType):
        sequence (int): Monotonic per-session event counter.
        text (str): Current hypothesis for the segment.
        stable_text (str): Prefix of `text` that agreed with the previous hypothesis. Equals `text` for final events.
        start_ms (int): Segment start in the session timeline, in milliseconds.
        end_ms (int):
        language (str | Unset):
        words (list[InferenceDictationWord] | Unset): Word spans on the session timeline. Empty for partial events.
    """

    object_: InferenceTranscriptionEventObject
    type_: InferenceTranscriptionEventType
    sequence: int
    text: str
    stable_text: str
    start_ms: int
    end_ms: int
    language: str | Unset = UNSET
    words: list[InferenceDictationWord] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        type_ = self.type_.value

        sequence = self.sequence

        text = self.text

        stable_text = self.stable_text

        start_ms = self.start_ms

        end_ms = self.end_ms

        language = self.language

        words: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.words, Unset):
            words = []
            for words_item_data in self.words:
                words_item = words_item_data.to_dict()
                words.append(words_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "type": type_,
                "sequence": sequence,
                "text": text,
                "stable_text": stable_text,
                "start_ms": start_ms,
                "end_ms": end_ms,
            }
        )
        if language is not UNSET:
            field_dict["language"] = language
        if words is not UNSET:
            field_dict["words"] = words

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_dictation_word import InferenceDictationWord

        d = dict(src_dict)
        object_ = InferenceTranscriptionEventObject(d.pop("object"))

        type_ = InferenceTranscriptionEventType(d.pop("type"))

        sequence = d.pop("sequence")

        text = d.pop("text")

        stable_text = d.pop("stable_text")

        start_ms = d.pop("start_ms")

        end_ms = d.pop("end_ms")

        language = d.pop("language", UNSET)

        _words = d.pop("words", UNSET)
        words: list[InferenceDictationWord] | Unset = UNSET
        if _words is not UNSET:
            words = []
            for words_item_data in _words:
                words_item = InferenceDictationWord.from_dict(words_item_data)

                words.append(words_item)

        inference_transcription_event = cls(
            object_=object_,
            type_=type_,
            sequence=sequence,
            text=text,
            stable_text=stable_text,
            start_ms=start_ms,
            end_ms=end_ms,
            language=language,
            words=words,
        )

        inference_transcription_event.additional_properties = d
        return inference_transcription_event

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
