from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.document_extraction_native_text_config_policy import DocumentExtractionNativeTextConfigPolicy
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_extraction_ocr_quality_config import DocumentExtractionOcrQualityConfig


T = TypeVar("T", bound="DocumentExtractionNativeTextConfig")


@_attrs_define
class DocumentExtractionNativeTextConfig:
    """Native PDF text policy and quality assessment. No OCR model is required. Native quality diagnostics are recorded
    independently of extraction completion and OCR availability.

        Attributes:
            policy (DocumentExtractionNativeTextConfigPolicy | Unset): `source` retains decoded source text, including off-
                page runs, for auditing. `page` excludes runs wholly outside the page or a known clipping box while retaining
                hidden OCR/accessibility text. `visible` additionally excludes nonpainting and fully transparent text. These
                policies do not claim pixel-perfect occlusion analysis. All policies honor semantic ActualText replacements.
                Default: DocumentExtractionNativeTextConfigPolicy.PAGE.
            quality (DocumentExtractionOcrQualityConfig | Unset): Text-quality thresholds. `native_text.quality` assesses
                native PDF text and selects fallback independently of whether OCR is configured; `ocr.quality` assesses OCR
                output. Quality flags are not guarantees of correct reading order or complete document semantics.
    """

    policy: DocumentExtractionNativeTextConfigPolicy | Unset = DocumentExtractionNativeTextConfigPolicy.PAGE
    quality: DocumentExtractionOcrQualityConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        policy: str | Unset = UNSET
        if not isinstance(self.policy, Unset):
            policy = self.policy.value

        quality: dict[str, Any] | Unset = UNSET
        if not isinstance(self.quality, Unset):
            quality = self.quality.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if policy is not UNSET:
            field_dict["policy"] = policy
        if quality is not UNSET:
            field_dict["quality"] = quality

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_extraction_ocr_quality_config import DocumentExtractionOcrQualityConfig

        d = dict(src_dict)
        _policy = d.pop("policy", UNSET)
        policy: DocumentExtractionNativeTextConfigPolicy | Unset
        if isinstance(_policy, Unset):
            policy = UNSET
        else:
            policy = DocumentExtractionNativeTextConfigPolicy(_policy)

        _quality = d.pop("quality", UNSET)
        quality: DocumentExtractionOcrQualityConfig | Unset
        if isinstance(_quality, Unset):
            quality = UNSET
        else:
            quality = DocumentExtractionOcrQualityConfig.from_dict(_quality)

        document_extraction_native_text_config = cls(
            policy=policy,
            quality=quality,
        )

        document_extraction_native_text_config.additional_properties = d
        return document_extraction_native_text_config

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
