from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.inference_dictation_word import InferenceDictationWord


T = TypeVar("T", bound="InferenceDictationSegment")


@_attrs_define
class InferenceDictationSegment:
    """One phrase bracketed by Whisper timestamp tokens (about 20 ms resolution).

    Attributes:
        text (str):
        start_ms (int): Phrase start offset in the clip, in milliseconds.
        end_ms (int): Phrase end offset in the clip, in milliseconds.
        words (list[InferenceDictationWord]): Word spans estimated inside the phrase by distributing its duration over
            word lengths.
    """

    text: str
    start_ms: int
    end_ms: int
    words: list[InferenceDictationWord]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        text = self.text

        start_ms = self.start_ms

        end_ms = self.end_ms

        words = []
        for words_item_data in self.words:
            words_item = words_item_data.to_dict()
            words.append(words_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "text": text,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "words": words,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_dictation_word import InferenceDictationWord

        d = dict(src_dict)
        text = d.pop("text")

        start_ms = d.pop("start_ms")

        end_ms = d.pop("end_ms")

        words = []
        _words = d.pop("words")
        for words_item_data in _words:
            words_item = InferenceDictationWord.from_dict(words_item_data)

            words.append(words_item)

        inference_dictation_segment = cls(
            text=text,
            start_ms=start_ms,
            end_ms=end_ms,
            words=words,
        )

        inference_dictation_segment.additional_properties = d
        return inference_dictation_segment

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
