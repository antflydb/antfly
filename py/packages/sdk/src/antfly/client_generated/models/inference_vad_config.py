from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceVadConfig")


@_attrs_define
class InferenceVadConfig:
    """Voice activity detection. Without `model`, frames are classified by
    RMS energy against `threshold`. With `model` naming a pulled Silero
    VAD export (`antfly inference pull onnx-community/silero-vad --tasks vad`),
    512-sample frames at 16 kHz are scored by the neural model, which
    separates speech from tones, music, and keyboard noise that the
    energy rule accepts.

        Attributes:
            model (str | Unset): Silero VAD model directory name from models_dir, for example `onnx-community/silero-vad`.
                Example: onnx-community/silero-vad.
            silero_threshold (float | Unset): Speech probability at or above which a Silero frame counts as speech. Default
                0.5.
            threshold (float | Unset): RMS amplitude on [-1, 1] PCM at or above which a 20 ms frame counts as speech.
                Default 0.012 (about -38 dBFS).
            min_speech_ms (int | Unset): Consecutive speech needed to open a segment. Default 120.
            min_silence_ms (int | Unset): Continuous silence that closes a segment. Default 600.
            speech_pad_ms (int | Unset): Padding kept on both sides of each segment. Default 120.
    """

    model: str | Unset = UNSET
    silero_threshold: float | Unset = UNSET
    threshold: float | Unset = UNSET
    min_speech_ms: int | Unset = UNSET
    min_silence_ms: int | Unset = UNSET
    speech_pad_ms: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        silero_threshold = self.silero_threshold

        threshold = self.threshold

        min_speech_ms = self.min_speech_ms

        min_silence_ms = self.min_silence_ms

        speech_pad_ms = self.speech_pad_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if model is not UNSET:
            field_dict["model"] = model
        if silero_threshold is not UNSET:
            field_dict["silero_threshold"] = silero_threshold
        if threshold is not UNSET:
            field_dict["threshold"] = threshold
        if min_speech_ms is not UNSET:
            field_dict["min_speech_ms"] = min_speech_ms
        if min_silence_ms is not UNSET:
            field_dict["min_silence_ms"] = min_silence_ms
        if speech_pad_ms is not UNSET:
            field_dict["speech_pad_ms"] = speech_pad_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model", UNSET)

        silero_threshold = d.pop("silero_threshold", UNSET)

        threshold = d.pop("threshold", UNSET)

        min_speech_ms = d.pop("min_speech_ms", UNSET)

        min_silence_ms = d.pop("min_silence_ms", UNSET)

        speech_pad_ms = d.pop("speech_pad_ms", UNSET)

        inference_vad_config = cls(
            model=model,
            silero_threshold=silero_threshold,
            threshold=threshold,
            min_speech_ms=min_speech_ms,
            min_silence_ms=min_silence_ms,
            speech_pad_ms=speech_pad_ms,
        )

        inference_vad_config.additional_properties = d
        return inference_vad_config

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
