from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_extraction_config_route_preset import DocumentExtractionConfigRoutePreset
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_extraction_ocr_config import DocumentExtractionOcrConfig
    from ..models.document_extraction_route import DocumentExtractionRoute
    from ..models.document_extraction_source_config import DocumentExtractionSourceConfig
    from ..models.document_extraction_transcription_config import DocumentExtractionTranscriptionConfig


T = TypeVar("T", bound="DocumentExtractionConfig")


@_attrs_define
class DocumentExtractionConfig:
    """Configuration for the `document_extraction` producer. It downloads the source URL and persists derived units such as
    PDF pages, sections, images, and audio.

    Native text (PDF text layers, Office documents, HTML, email, plain text) is extracted with no model. Scanned pages
    and images are read only when `ocr` names a reader or generator, and audio is transcribed only when `transcription`
    names a transcriber. Units that need a producer that is not configured report `extraction_status`
    `ocr_not_configured` or `transcription_not_configured`.

    **Example:**
    ```json
    {
      "source": {"filename_field": "filename", "content_type_field": "mime_type"},
      "ocr": {"reader": {"provider": "antfly", "model": "lightonai/LightOnOCR-3-0.8B"}},
      "transcription": {"transcriber": {"provider": "antfly", "model": "openai/whisper-small"}}
    }
    ```

        Attributes:
            source (DocumentExtractionSourceConfig | Unset): Names of row fields that carry source metadata. Each overrides
                the matching static field.
            filename (str | Unset): Static filename used for content detection when the row has none.
            content_type (str | Unset): Static content type. Overrides the downloaded response's content type.
            etag (str | Unset): Static ETag recorded as source version metadata.
            checksum (str | Unset): Static checksum recorded as source version metadata.
            version (str | Unset): Static source version. Together with the other metadata it lets extraction skip unchanged
                sources.
            last_modified (str | Unset): Static last-modified timestamp recorded as source version metadata.
            credentials (str | Unset): Credential reference used to download the source URL.
            html_strip_tags (bool | Unset): Strip HTML markup from HTML sources before storing their text. Default: True.
            ocr (DocumentExtractionOcrConfig | Unset): Reads scanned pages and images. Name exactly one of `reader` or
                `generator`. Readers run their own OCR task; generators receive a transcription instruction.
            transcription (DocumentExtractionTranscriptionConfig | Unset): Transcribes audio units.
            route_preset (DocumentExtractionConfigRoutePreset | Unset): How sources that match no explicit route are
                handled. `mixed_files` (alias `default`) detects PDFs, HTML, email, Office documents, ZIP archives, images,
                audio, and text. `explicit_only` (alias `none`) records unmatched sources as unsupported. Detection never
                requests a producer: detected images and audio without a configured producer report `ocr_not_configured` or
                `transcription_not_configured`. Default: DocumentExtractionConfigRoutePreset.MIXED_FILES.
            routes (list[DocumentExtractionRoute] | Unset): Routes evaluated in order before the preset. The first route
                whose match applies selects the extractor.
    """

    source: DocumentExtractionSourceConfig | Unset = UNSET
    filename: str | Unset = UNSET
    content_type: str | Unset = UNSET
    etag: str | Unset = UNSET
    checksum: str | Unset = UNSET
    version: str | Unset = UNSET
    last_modified: str | Unset = UNSET
    credentials: str | Unset = UNSET
    html_strip_tags: bool | Unset = True
    ocr: DocumentExtractionOcrConfig | Unset = UNSET
    transcription: DocumentExtractionTranscriptionConfig | Unset = UNSET
    route_preset: DocumentExtractionConfigRoutePreset | Unset = DocumentExtractionConfigRoutePreset.MIXED_FILES
    routes: list[DocumentExtractionRoute] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        source: dict[str, Any] | Unset = UNSET
        if not isinstance(self.source, Unset):
            source = self.source.to_dict()

        filename = self.filename

        content_type = self.content_type

        etag = self.etag

        checksum = self.checksum

        version = self.version

        last_modified = self.last_modified

        credentials = self.credentials

        html_strip_tags = self.html_strip_tags

        ocr: dict[str, Any] | Unset = UNSET
        if not isinstance(self.ocr, Unset):
            ocr = self.ocr.to_dict()

        transcription: dict[str, Any] | Unset = UNSET
        if not isinstance(self.transcription, Unset):
            transcription = self.transcription.to_dict()

        route_preset: str | Unset = UNSET
        if not isinstance(self.route_preset, Unset):
            route_preset = self.route_preset.value

        routes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.routes, Unset):
            routes = []
            for routes_item_data in self.routes:
                routes_item = routes_item_data.to_dict()
                routes.append(routes_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if source is not UNSET:
            field_dict["source"] = source
        if filename is not UNSET:
            field_dict["filename"] = filename
        if content_type is not UNSET:
            field_dict["content_type"] = content_type
        if etag is not UNSET:
            field_dict["etag"] = etag
        if checksum is not UNSET:
            field_dict["checksum"] = checksum
        if version is not UNSET:
            field_dict["version"] = version
        if last_modified is not UNSET:
            field_dict["last_modified"] = last_modified
        if credentials is not UNSET:
            field_dict["credentials"] = credentials
        if html_strip_tags is not UNSET:
            field_dict["html_strip_tags"] = html_strip_tags
        if ocr is not UNSET:
            field_dict["ocr"] = ocr
        if transcription is not UNSET:
            field_dict["transcription"] = transcription
        if route_preset is not UNSET:
            field_dict["route_preset"] = route_preset
        if routes is not UNSET:
            field_dict["routes"] = routes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_extraction_ocr_config import DocumentExtractionOcrConfig
        from ..models.document_extraction_route import DocumentExtractionRoute
        from ..models.document_extraction_source_config import DocumentExtractionSourceConfig
        from ..models.document_extraction_transcription_config import DocumentExtractionTranscriptionConfig

        d = dict(src_dict)
        _source = d.pop("source", UNSET)
        source: DocumentExtractionSourceConfig | Unset
        if isinstance(_source, Unset):
            source = UNSET
        else:
            source = DocumentExtractionSourceConfig.from_dict(_source)

        filename = d.pop("filename", UNSET)

        content_type = d.pop("content_type", UNSET)

        etag = d.pop("etag", UNSET)

        checksum = d.pop("checksum", UNSET)

        version = d.pop("version", UNSET)

        last_modified = d.pop("last_modified", UNSET)

        credentials = d.pop("credentials", UNSET)

        html_strip_tags = d.pop("html_strip_tags", UNSET)

        _ocr = d.pop("ocr", UNSET)
        ocr: DocumentExtractionOcrConfig | Unset
        if isinstance(_ocr, Unset):
            ocr = UNSET
        else:
            ocr = DocumentExtractionOcrConfig.from_dict(_ocr)

        _transcription = d.pop("transcription", UNSET)
        transcription: DocumentExtractionTranscriptionConfig | Unset
        if isinstance(_transcription, Unset):
            transcription = UNSET
        else:
            transcription = DocumentExtractionTranscriptionConfig.from_dict(_transcription)

        _route_preset = d.pop("route_preset", UNSET)
        route_preset: DocumentExtractionConfigRoutePreset | Unset
        if isinstance(_route_preset, Unset):
            route_preset = UNSET
        else:
            route_preset = DocumentExtractionConfigRoutePreset(_route_preset)

        _routes = d.pop("routes", UNSET)
        routes: list[DocumentExtractionRoute] | Unset = UNSET
        if _routes is not UNSET:
            routes = []
            for routes_item_data in _routes:
                routes_item = DocumentExtractionRoute.from_dict(routes_item_data)

                routes.append(routes_item)

        document_extraction_config = cls(
            source=source,
            filename=filename,
            content_type=content_type,
            etag=etag,
            checksum=checksum,
            version=version,
            last_modified=last_modified,
            credentials=credentials,
            html_strip_tags=html_strip_tags,
            ocr=ocr,
            transcription=transcription,
            route_preset=route_preset,
            routes=routes,
        )

        document_extraction_config.additional_properties = d
        return document_extraction_config

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
