from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_long_document_options_mode import ExtractionLongDocumentOptionsMode
from ..models.extraction_long_document_options_record_identity import ExtractionLongDocumentOptionsRecordIdentity
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionLongDocumentOptions")


@_attrs_define
class ExtractionLongDocumentOptions:
    """Version 2 never silently truncates. Reject is the default. Windowing requires an enabled runtime capability,
    reconstructs document-global offsets and revalidates all hard graph constraints after merging.

        Attributes:
            mode (ExtractionLongDocumentOptionsMode | Unset):  Default: ExtractionLongDocumentOptionsMode.REJECT.
            window_words (int | Unset): Maximum body words per window; also bounded by the checkpoint and encoded token
                limits.
            overlap_words (int | Unset):
            max_windows (int | Unset):
            record_identity (ExtractionLongDocumentOptionsRecordIdentity | Unset): Identity of latent, anchorless and legacy
                records across windows. Occurrence uses exact source spans; semantic explicitly merges equal field values.
                Natural records always use their exact source anchor. This is independent of annotation occurrence_policy.
                Default: ExtractionLongDocumentOptionsRecordIdentity.OCCURRENCE.
    """

    mode: ExtractionLongDocumentOptionsMode | Unset = ExtractionLongDocumentOptionsMode.REJECT
    window_words: int | Unset = UNSET
    overlap_words: int | Unset = UNSET
    max_windows: int | Unset = UNSET
    record_identity: ExtractionLongDocumentOptionsRecordIdentity | Unset = (
        ExtractionLongDocumentOptionsRecordIdentity.OCCURRENCE
    )

    def to_dict(self) -> dict[str, Any]:
        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        window_words = self.window_words

        overlap_words = self.overlap_words

        max_windows = self.max_windows

        record_identity: str | Unset = UNSET
        if not isinstance(self.record_identity, Unset):
            record_identity = self.record_identity.value

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
        if record_identity is not UNSET:
            field_dict["record_identity"] = record_identity

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _mode = d.pop("mode", UNSET)
        mode: ExtractionLongDocumentOptionsMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = ExtractionLongDocumentOptionsMode(_mode)

        window_words = d.pop("window_words", UNSET)

        overlap_words = d.pop("overlap_words", UNSET)

        max_windows = d.pop("max_windows", UNSET)

        _record_identity = d.pop("record_identity", UNSET)
        record_identity: ExtractionLongDocumentOptionsRecordIdentity | Unset
        if isinstance(_record_identity, Unset):
            record_identity = UNSET
        else:
            record_identity = ExtractionLongDocumentOptionsRecordIdentity(_record_identity)

        extraction_long_document_options = cls(
            mode=mode,
            window_words=window_words,
            overlap_words=overlap_words,
            max_windows=max_windows,
            record_identity=record_identity,
        )

        return extraction_long_document_options
