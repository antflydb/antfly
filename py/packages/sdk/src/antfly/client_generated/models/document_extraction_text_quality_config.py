from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentExtractionTextQualityConfig")


@_attrs_define
class DocumentExtractionTextQualityConfig:
    """Text-quality thresholds. `native_text.quality` assesses native document text and selects fallback independently of
    whether OCR is configured; `ocr.quality` assesses OCR output. Quality flags are not guarantees of correct reading
    order or complete document semantics.

        Attributes:
            min_chars (int | Unset): Minimum non-whitespace Unicode characters before text passes the length check. Default:
                50.
            garbled_min_words (int | Unset): Minimum words before the garbled-text check applies. Default: 20.
            garbled_sample_words (int | Unset): Words sampled by the garbled-text check. Default: 50.
            max_single_char_word_ratio (float | Unset): Maximum ratio of single-character words before text counts as
                garbled. Default: 0.4.
            min_line_chars (int | Unset): Minimum non-whitespace Unicode characters for a line to count in the corrupted-
                line check. Default: 15.
            max_corrupted_line_ratio (float | Unset): Maximum ratio of corrupted lines. Default: 0.2.
            max_font_corruption (float | Unset): Font corruption score above which a substantial line counts as corrupted.
                Default: 0.5.
            max_replacement_ratio (float | Unset): Maximum ratio of Unicode replacement characters. Default: 0.05.
    """

    min_chars: int | Unset = 50
    garbled_min_words: int | Unset = 20
    garbled_sample_words: int | Unset = 50
    max_single_char_word_ratio: float | Unset = 0.4
    min_line_chars: int | Unset = 15
    max_corrupted_line_ratio: float | Unset = 0.2
    max_font_corruption: float | Unset = 0.5
    max_replacement_ratio: float | Unset = 0.05
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        min_chars = self.min_chars

        garbled_min_words = self.garbled_min_words

        garbled_sample_words = self.garbled_sample_words

        max_single_char_word_ratio = self.max_single_char_word_ratio

        min_line_chars = self.min_line_chars

        max_corrupted_line_ratio = self.max_corrupted_line_ratio

        max_font_corruption = self.max_font_corruption

        max_replacement_ratio = self.max_replacement_ratio

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if min_chars is not UNSET:
            field_dict["min_chars"] = min_chars
        if garbled_min_words is not UNSET:
            field_dict["garbled_min_words"] = garbled_min_words
        if garbled_sample_words is not UNSET:
            field_dict["garbled_sample_words"] = garbled_sample_words
        if max_single_char_word_ratio is not UNSET:
            field_dict["max_single_char_word_ratio"] = max_single_char_word_ratio
        if min_line_chars is not UNSET:
            field_dict["min_line_chars"] = min_line_chars
        if max_corrupted_line_ratio is not UNSET:
            field_dict["max_corrupted_line_ratio"] = max_corrupted_line_ratio
        if max_font_corruption is not UNSET:
            field_dict["max_font_corruption"] = max_font_corruption
        if max_replacement_ratio is not UNSET:
            field_dict["max_replacement_ratio"] = max_replacement_ratio

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        min_chars = d.pop("min_chars", UNSET)

        garbled_min_words = d.pop("garbled_min_words", UNSET)

        garbled_sample_words = d.pop("garbled_sample_words", UNSET)

        max_single_char_word_ratio = d.pop("max_single_char_word_ratio", UNSET)

        min_line_chars = d.pop("min_line_chars", UNSET)

        max_corrupted_line_ratio = d.pop("max_corrupted_line_ratio", UNSET)

        max_font_corruption = d.pop("max_font_corruption", UNSET)

        max_replacement_ratio = d.pop("max_replacement_ratio", UNSET)

        document_extraction_text_quality_config = cls(
            min_chars=min_chars,
            garbled_min_words=garbled_min_words,
            garbled_sample_words=garbled_sample_words,
            max_single_char_word_ratio=max_single_char_word_ratio,
            min_line_chars=min_line_chars,
            max_corrupted_line_ratio=max_corrupted_line_ratio,
            max_font_corruption=max_font_corruption,
            max_replacement_ratio=max_replacement_ratio,
        )

        document_extraction_text_quality_config.additional_properties = d
        return document_extraction_text_quality_config

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
