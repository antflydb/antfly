from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_vad_options import InferenceVADOptions


T = TypeVar("T", bound="InferenceAudioChunkConfig")


@_attrs_define
class InferenceAudioChunkConfig:
    """Audio chunking configuration for inference, including VAD options.

    Attributes:
        window_duration_ms (int | Unset): Window duration in milliseconds for fixed-window audio chunking (default:
            30000).
        overlap_duration_ms (int | Unset): Overlap duration in milliseconds between audio chunks (default: 0).
        vad (InferenceVADOptions | Unset): Options for Voice Activity Detection (VAD) based audio segmentation.
            inference-specific.
    """

    window_duration_ms: int | Unset = UNSET
    overlap_duration_ms: int | Unset = UNSET
    vad: InferenceVADOptions | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        window_duration_ms = self.window_duration_ms

        overlap_duration_ms = self.overlap_duration_ms

        vad: dict[str, Any] | Unset = UNSET
        if not isinstance(self.vad, Unset):
            vad = self.vad.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if window_duration_ms is not UNSET:
            field_dict["window_duration_ms"] = window_duration_ms
        if overlap_duration_ms is not UNSET:
            field_dict["overlap_duration_ms"] = overlap_duration_ms
        if vad is not UNSET:
            field_dict["vad"] = vad

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_vad_options import InferenceVADOptions

        d = dict(src_dict)
        window_duration_ms = d.pop("window_duration_ms", UNSET)

        overlap_duration_ms = d.pop("overlap_duration_ms", UNSET)

        _vad = d.pop("vad", UNSET)
        vad: InferenceVADOptions | Unset
        if isinstance(_vad, Unset):
            vad = UNSET
        else:
            vad = InferenceVADOptions.from_dict(_vad)

        inference_audio_chunk_config = cls(
            window_duration_ms=window_duration_ms,
            overlap_duration_ms=overlap_duration_ms,
            vad=vad,
        )

        inference_audio_chunk_config.additional_properties = d
        return inference_audio_chunk_config

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
