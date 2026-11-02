from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_extraction_ocr_config_mode import DocumentExtractionOcrConfigMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_extraction_ocr_quality_config import DocumentExtractionOcrQualityConfig
    from ..models.document_extraction_reader_config import DocumentExtractionReaderConfig
    from ..models.document_extraction_render_config import DocumentExtractionRenderConfig
    from ..models.generator_config import GeneratorConfig


T = TypeVar("T", bound="DocumentExtractionOcrConfig")


@_attrs_define
class DocumentExtractionOcrConfig:
    """Reads scanned pages and images. Name exactly one of `reader` or `generator`. Readers run their own OCR task;
    generators receive a transcription instruction.

        Attributes:
            reader (DocumentExtractionReaderConfig | Unset): Reader provider for OCR. Every provider except `apple` requires
                `model`; Apple uses Vision text recognition and accepts no model, credentials, or prompt.
            generator (GeneratorConfig | Unset): A unified configuration for a generative AI provider.
                 Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
            mode (DocumentExtractionOcrConfigMode | Unset): `auto` reads PDF pages whose native text fails the `quality`
                checks, and images. `always` reads every page. Default: DocumentExtractionOcrConfigMode.AUTO.
            prompt (str | Unset): Overrides the OCR prompt. Omit it to let the reader run its own OCR task. Apple readers
                reject prompts.
            render (DocumentExtractionRenderConfig | Unset): How PDF pages are rasterized for OCR.
            quality (DocumentExtractionOcrQualityConfig | Unset): Thresholds that decide when a PDF page's native text is
                too poor to keep, in `auto` mode.
    """

    reader: DocumentExtractionReaderConfig | Unset = UNSET
    generator: GeneratorConfig | Unset = UNSET
    mode: DocumentExtractionOcrConfigMode | Unset = DocumentExtractionOcrConfigMode.AUTO
    prompt: str | Unset = UNSET
    render: DocumentExtractionRenderConfig | Unset = UNSET
    quality: DocumentExtractionOcrQualityConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        reader: dict[str, Any] | Unset = UNSET
        if not isinstance(self.reader, Unset):
            reader = self.reader.to_dict()

        generator: dict[str, Any] | Unset = UNSET
        if not isinstance(self.generator, Unset):
            generator = self.generator.to_dict()

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        prompt = self.prompt

        render: dict[str, Any] | Unset = UNSET
        if not isinstance(self.render, Unset):
            render = self.render.to_dict()

        quality: dict[str, Any] | Unset = UNSET
        if not isinstance(self.quality, Unset):
            quality = self.quality.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if reader is not UNSET:
            field_dict["reader"] = reader
        if generator is not UNSET:
            field_dict["generator"] = generator
        if mode is not UNSET:
            field_dict["mode"] = mode
        if prompt is not UNSET:
            field_dict["prompt"] = prompt
        if render is not UNSET:
            field_dict["render"] = render
        if quality is not UNSET:
            field_dict["quality"] = quality

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_extraction_ocr_quality_config import DocumentExtractionOcrQualityConfig
        from ..models.document_extraction_reader_config import DocumentExtractionReaderConfig
        from ..models.document_extraction_render_config import DocumentExtractionRenderConfig
        from ..models.generator_config import GeneratorConfig

        d = dict(src_dict)
        _reader = d.pop("reader", UNSET)
        reader: DocumentExtractionReaderConfig | Unset
        if isinstance(_reader, Unset):
            reader = UNSET
        else:
            reader = DocumentExtractionReaderConfig.from_dict(_reader)

        _generator = d.pop("generator", UNSET)
        generator: GeneratorConfig | Unset
        if isinstance(_generator, Unset):
            generator = UNSET
        else:
            generator = GeneratorConfig.from_dict(_generator)

        _mode = d.pop("mode", UNSET)
        mode: DocumentExtractionOcrConfigMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = DocumentExtractionOcrConfigMode(_mode)

        prompt = d.pop("prompt", UNSET)

        _render = d.pop("render", UNSET)
        render: DocumentExtractionRenderConfig | Unset
        if isinstance(_render, Unset):
            render = UNSET
        else:
            render = DocumentExtractionRenderConfig.from_dict(_render)

        _quality = d.pop("quality", UNSET)
        quality: DocumentExtractionOcrQualityConfig | Unset
        if isinstance(_quality, Unset):
            quality = UNSET
        else:
            quality = DocumentExtractionOcrQualityConfig.from_dict(_quality)

        document_extraction_ocr_config = cls(
            reader=reader,
            generator=generator,
            mode=mode,
            prompt=prompt,
            render=render,
            quality=quality,
        )

        document_extraction_ocr_config.additional_properties = d
        return document_extraction_ocr_config

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
