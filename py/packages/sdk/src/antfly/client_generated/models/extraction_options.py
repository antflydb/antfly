from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.extraction_offset_unit import ExtractionOffsetUnit
from ..models.extraction_options_overlap import ExtractionOptionsOverlap
from ..models.extraction_options_word_splitter import ExtractionOptionsWordSplitter
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_decoder_options import ExtractionDecoderOptions
    from ..models.extraction_joint_options import ExtractionJointOptions
    from ..models.extraction_long_document_options import ExtractionLongDocumentOptions
    from ..models.extraction_reader_options import ExtractionReaderOptions
    from ..models.extraction_resolver_options import ExtractionResolverOptions


T = TypeVar("T", bound="ExtractionOptions")


@_attrs_define
class ExtractionOptions:
    """
    Attributes:
        threshold (float | Unset):
        flat_ner (bool | Unset):
        include_confidence (bool | Unset):
        include_spans (bool | Unset):
        word_splitter (ExtractionOptionsWordSplitter | Unset): Version 2 source word splitting. char keeps ASCII
            alphanumeric and @._-+ runs together and splits other non-whitespace codepoints, preserving original source
            offsets. An input's options replace the shared options in full; omitted word_splitter uses whitespace. Explicit
            word_splitter is rejected by version 1.
        overlap (ExtractionOptionsOverlap | Unset): Version 2 overlap selection. flat/disallow prohibit overlap, nested
            permits containment, longest removes strictly contained spans.
        offset_unit (ExtractionOffsetUnit | Unset): Half-open offsets into the immutable caller text. Version 2 defaults
            to utf8_bytes. No normalization, lowercasing or synthetic suffix is included in these coordinates.
        long_document (ExtractionLongDocumentOptions | Unset): Version 2 never silently truncates. Reject is the
            default. Windowing requires an enabled runtime capability, reconstructs document-global offsets and revalidates
            all hard graph constraints after merging.
        decoder (ExtractionDecoderOptions | Unset): Bounded classification and JointIE selection. Exact optimality is
            with respect to admitted candidates. A completed beam may be feasible without an optimality proof. By default
            exhausted search is an error; best_effort permits only a validated feasible witness and reports exhausted:true.
        joint_ie (ExtractionJointOptions | Unset): JointIE proposal admission and utility calibration. Entity candidate
            caps are bypassed for endpoints of retained relation proposals, subject to server hard bounds. entity_threshold
            overrides candidate admission, not entity decision thresholds.
        reader (ExtractionReaderOptions | Unset):
        resolver (ExtractionResolverOptions | Unset): Optional cross-input entity and relation deduplication.
    """

    threshold: float | Unset = UNSET
    flat_ner: bool | Unset = UNSET
    include_confidence: bool | Unset = UNSET
    include_spans: bool | Unset = UNSET
    word_splitter: ExtractionOptionsWordSplitter | Unset = UNSET
    overlap: ExtractionOptionsOverlap | Unset = UNSET
    offset_unit: ExtractionOffsetUnit | Unset = UNSET
    long_document: ExtractionLongDocumentOptions | Unset = UNSET
    decoder: ExtractionDecoderOptions | Unset = UNSET
    joint_ie: ExtractionJointOptions | Unset = UNSET
    reader: ExtractionReaderOptions | Unset = UNSET
    resolver: ExtractionResolverOptions | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        threshold = self.threshold

        flat_ner = self.flat_ner

        include_confidence = self.include_confidence

        include_spans = self.include_spans

        word_splitter: str | Unset = UNSET
        if not isinstance(self.word_splitter, Unset):
            word_splitter = self.word_splitter.value

        overlap: str | Unset = UNSET
        if not isinstance(self.overlap, Unset):
            overlap = self.overlap.value

        offset_unit: str | Unset = UNSET
        if not isinstance(self.offset_unit, Unset):
            offset_unit = self.offset_unit.value

        long_document: dict[str, Any] | Unset = UNSET
        if not isinstance(self.long_document, Unset):
            long_document = self.long_document.to_dict()

        decoder: dict[str, Any] | Unset = UNSET
        if not isinstance(self.decoder, Unset):
            decoder = self.decoder.to_dict()

        joint_ie: dict[str, Any] | Unset = UNSET
        if not isinstance(self.joint_ie, Unset):
            joint_ie = self.joint_ie.to_dict()

        reader: dict[str, Any] | Unset = UNSET
        if not isinstance(self.reader, Unset):
            reader = self.reader.to_dict()

        resolver: dict[str, Any] | Unset = UNSET
        if not isinstance(self.resolver, Unset):
            resolver = self.resolver.to_dict()

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
        if word_splitter is not UNSET:
            field_dict["word_splitter"] = word_splitter
        if overlap is not UNSET:
            field_dict["overlap"] = overlap
        if offset_unit is not UNSET:
            field_dict["offset_unit"] = offset_unit
        if long_document is not UNSET:
            field_dict["long_document"] = long_document
        if decoder is not UNSET:
            field_dict["decoder"] = decoder
        if joint_ie is not UNSET:
            field_dict["joint_ie"] = joint_ie
        if reader is not UNSET:
            field_dict["reader"] = reader
        if resolver is not UNSET:
            field_dict["resolver"] = resolver

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_decoder_options import ExtractionDecoderOptions
        from ..models.extraction_joint_options import ExtractionJointOptions
        from ..models.extraction_long_document_options import ExtractionLongDocumentOptions
        from ..models.extraction_reader_options import ExtractionReaderOptions
        from ..models.extraction_resolver_options import ExtractionResolverOptions

        d = dict(src_dict)
        threshold = d.pop("threshold", UNSET)

        flat_ner = d.pop("flat_ner", UNSET)

        include_confidence = d.pop("include_confidence", UNSET)

        include_spans = d.pop("include_spans", UNSET)

        _word_splitter = d.pop("word_splitter", UNSET)
        word_splitter: ExtractionOptionsWordSplitter | Unset
        if isinstance(_word_splitter, Unset):
            word_splitter = UNSET
        else:
            word_splitter = ExtractionOptionsWordSplitter(_word_splitter)

        _overlap = d.pop("overlap", UNSET)
        overlap: ExtractionOptionsOverlap | Unset
        if isinstance(_overlap, Unset):
            overlap = UNSET
        else:
            overlap = ExtractionOptionsOverlap(_overlap)

        _offset_unit = d.pop("offset_unit", UNSET)
        offset_unit: ExtractionOffsetUnit | Unset
        if isinstance(_offset_unit, Unset):
            offset_unit = UNSET
        else:
            offset_unit = ExtractionOffsetUnit(_offset_unit)

        _long_document = d.pop("long_document", UNSET)
        long_document: ExtractionLongDocumentOptions | Unset
        if isinstance(_long_document, Unset):
            long_document = UNSET
        else:
            long_document = ExtractionLongDocumentOptions.from_dict(_long_document)

        _decoder = d.pop("decoder", UNSET)
        decoder: ExtractionDecoderOptions | Unset
        if isinstance(_decoder, Unset):
            decoder = UNSET
        else:
            decoder = ExtractionDecoderOptions.from_dict(_decoder)

        _joint_ie = d.pop("joint_ie", UNSET)
        joint_ie: ExtractionJointOptions | Unset
        if isinstance(_joint_ie, Unset):
            joint_ie = UNSET
        else:
            joint_ie = ExtractionJointOptions.from_dict(_joint_ie)

        _reader = d.pop("reader", UNSET)
        reader: ExtractionReaderOptions | Unset
        if isinstance(_reader, Unset):
            reader = UNSET
        else:
            reader = ExtractionReaderOptions.from_dict(_reader)

        _resolver = d.pop("resolver", UNSET)
        resolver: ExtractionResolverOptions | Unset
        if isinstance(_resolver, Unset):
            resolver = UNSET
        else:
            resolver = ExtractionResolverOptions.from_dict(_resolver)

        extraction_options = cls(
            threshold=threshold,
            flat_ner=flat_ner,
            include_confidence=include_confidence,
            include_spans=include_spans,
            word_splitter=word_splitter,
            overlap=overlap,
            offset_unit=offset_unit,
            long_document=long_document,
            decoder=decoder,
            joint_ie=joint_ie,
            reader=reader,
            resolver=resolver,
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
