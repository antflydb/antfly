from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DocumentExtractionOcrQualityConfig")


@_attrs_define
class DocumentExtractionOcrQualityConfig:
    """Thresholds that decide when a PDF page's native text is too poor to keep, in `auto` mode.

    Attributes:
        min_content_chars (int | Unset): Pages with fewer meaningful characters are read with OCR. Default: 50.
        garbled_min_words (int | Unset): Minimum words before the garbled-text check applies. Default: 20.
        garbled_sample_words (int | Unset): Words sampled by the garbled-text check. Default: 50.
        max_single_char_word_ratio (float | Unset): Maximum ratio of single-character words before text counts as
            garbled. Default: 0.4.
        substantial_line_min_chars (int | Unset): Minimum characters for a line to count in the corrupted-line check.
            Default: 15.
        max_corrupted_line_ratio (float | Unset): Maximum ratio of corrupted lines. Default: 0.2.
        font_corruption_score_threshold (float | Unset): Font corruption score at which text is replaced by OCR.
            Default: 0.5.
        max_replacement_char_ratio (float | Unset): Maximum ratio of Unicode replacement characters. Default: 0.05.
    """

    min_content_chars: int | Unset = 50
    garbled_min_words: int | Unset = 20
    garbled_sample_words: int | Unset = 50
    max_single_char_word_ratio: float | Unset = 0.4
    substantial_line_min_chars: int | Unset = 15
    max_corrupted_line_ratio: float | Unset = 0.2
    font_corruption_score_threshold: float | Unset = 0.5
    max_replacement_char_ratio: float | Unset = 0.05
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        min_content_chars = self.min_content_chars

        garbled_min_words = self.garbled_min_words

        garbled_sample_words = self.garbled_sample_words

        max_single_char_word_ratio = self.max_single_char_word_ratio

        substantial_line_min_chars = self.substantial_line_min_chars

        max_corrupted_line_ratio = self.max_corrupted_line_ratio

        font_corruption_score_threshold = self.font_corruption_score_threshold

        max_replacement_char_ratio = self.max_replacement_char_ratio

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if min_content_chars is not UNSET:
            field_dict["min_content_chars"] = min_content_chars
        if garbled_min_words is not UNSET:
            field_dict["garbled_min_words"] = garbled_min_words
        if garbled_sample_words is not UNSET:
            field_dict["garbled_sample_words"] = garbled_sample_words
        if max_single_char_word_ratio is not UNSET:
            field_dict["max_single_char_word_ratio"] = max_single_char_word_ratio
        if substantial_line_min_chars is not UNSET:
            field_dict["substantial_line_min_chars"] = substantial_line_min_chars
        if max_corrupted_line_ratio is not UNSET:
            field_dict["max_corrupted_line_ratio"] = max_corrupted_line_ratio
        if font_corruption_score_threshold is not UNSET:
            field_dict["font_corruption_score_threshold"] = font_corruption_score_threshold
        if max_replacement_char_ratio is not UNSET:
            field_dict["max_replacement_char_ratio"] = max_replacement_char_ratio

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        min_content_chars = d.pop("min_content_chars", UNSET)

        garbled_min_words = d.pop("garbled_min_words", UNSET)

        garbled_sample_words = d.pop("garbled_sample_words", UNSET)

        max_single_char_word_ratio = d.pop("max_single_char_word_ratio", UNSET)

        substantial_line_min_chars = d.pop("substantial_line_min_chars", UNSET)

        max_corrupted_line_ratio = d.pop("max_corrupted_line_ratio", UNSET)

        font_corruption_score_threshold = d.pop("font_corruption_score_threshold", UNSET)

        max_replacement_char_ratio = d.pop("max_replacement_char_ratio", UNSET)

        document_extraction_ocr_quality_config = cls(
            min_content_chars=min_content_chars,
            garbled_min_words=garbled_min_words,
            garbled_sample_words=garbled_sample_words,
            max_single_char_word_ratio=max_single_char_word_ratio,
            substantial_line_min_chars=substantial_line_min_chars,
            max_corrupted_line_ratio=max_corrupted_line_ratio,
            font_corruption_score_threshold=font_corruption_score_threshold,
            max_replacement_char_ratio=max_replacement_char_ratio,
        )

        document_extraction_ocr_quality_config.additional_properties = d
        return document_extraction_ocr_quality_config

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
