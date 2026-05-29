from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_reader_options import ExtractionReaderOptions


T = TypeVar("T", bound="ExtractionOptions")


@_attrs_define
class ExtractionOptions:
    """
    Attributes:
        threshold (float | Unset):
        flat_ner (bool | Unset):
        include_confidence (bool | Unset):
        include_spans (bool | Unset):
        reader (ExtractionReaderOptions | Unset):
    """

    threshold: float | Unset = UNSET
    flat_ner: bool | Unset = UNSET
    include_confidence: bool | Unset = UNSET
    include_spans: bool | Unset = UNSET
    reader: ExtractionReaderOptions | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        threshold = self.threshold

        flat_ner = self.flat_ner

        include_confidence = self.include_confidence

        include_spans = self.include_spans

        reader: dict[str, Any] | Unset = UNSET
        if not isinstance(self.reader, Unset):
            reader = self.reader.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if threshold is not UNSET:
            field_dict["threshold"] = threshold
        if flat_ner is not UNSET:
            field_dict["flat_ner"] = flat_ner
        if include_confidence is not UNSET:
            field_dict["include_confidence"] = include_confidence
        if include_spans is not UNSET:
            field_dict["include_spans"] = include_spans
        if reader is not UNSET:
            field_dict["reader"] = reader

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_reader_options import ExtractionReaderOptions

        d = dict(src_dict)
        threshold = d.pop("threshold", UNSET)

        flat_ner = d.pop("flat_ner", UNSET)

        include_confidence = d.pop("include_confidence", UNSET)

        include_spans = d.pop("include_spans", UNSET)

        _reader = d.pop("reader", UNSET)
        reader: ExtractionReaderOptions | Unset
        if isinstance(_reader, Unset):
            reader = UNSET
        else:
            reader = ExtractionReaderOptions.from_dict(_reader)

        extraction_options = cls(
            threshold=threshold,
            flat_ner=flat_ner,
            include_confidence=include_confidence,
            include_spans=include_spans,
            reader=reader,
        )

        extraction_options.additional_properties = d
        return extraction_options

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
