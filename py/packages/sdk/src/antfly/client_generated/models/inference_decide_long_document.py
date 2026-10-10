from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.inference_decide_long_document_mode import InferenceDecideLongDocumentMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceDecideLongDocument")


@_attrs_define
class InferenceDecideLongDocument:
    """Explicit windowing for qualified boundary decision models. Span and embedding decision models reject window mode.
    Omission preserves rejection of over-limit text.

        Attributes:
            mode (InferenceDecideLongDocumentMode | Unset):  Default: InferenceDecideLongDocumentMode.REJECT.
            window_words (int | Unset):  Default: 1024.
            overlap_words (int | Unset): Must be smaller than window_words. Only valid in window mode. Default: 32.
            max_windows (int | Unset):  Default: 128.
    """

    mode: InferenceDecideLongDocumentMode | Unset = InferenceDecideLongDocumentMode.REJECT
    window_words: int | Unset = 1024
    overlap_words: int | Unset = 32
    max_windows: int | Unset = 128

    def to_dict(self) -> dict[str, Any]:
        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        window_words = self.window_words

        overlap_words = self.overlap_words

        max_windows = self.max_windows

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if mode is not UNSET:
            field_dict["mode"] = mode
        if window_words is not UNSET:
            field_dict["window_words"] = window_words
        if overlap_words is not UNSET:
            field_dict["overlap_words"] = overlap_words
        if max_windows is not UNSET:
            field_dict["max_windows"] = max_windows

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _mode = d.pop("mode", UNSET)
        mode: InferenceDecideLongDocumentMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = InferenceDecideLongDocumentMode(_mode)

        window_words = d.pop("window_words", UNSET)

        overlap_words = d.pop("overlap_words", UNSET)

        max_windows = d.pop("max_windows", UNSET)

        inference_decide_long_document = cls(
            mode=mode,
            window_words=window_words,
            overlap_words=overlap_words,
            max_windows=max_windows,
        )

        return inference_decide_long_document
