from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_audio_context import InferenceAudioContext
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_vad_config import InferenceVadConfig


T = TypeVar("T", bound="InferenceTranscriptionSessionRequest")


@_attrs_define
class InferenceTranscriptionSessionRequest:
    """
    Attributes:
        model (str): Transcriber model from models_dir/transcribers/. Example: openai/whisper-tiny.
        language (str | Unset): Force the transcript language (ISO 639-1). Omit for automatic detection.
        vad (InferenceVadConfig | Unset): Voice activity detection. Without `model`, frames are classified by
            RMS energy against `threshold`. With `model` naming a pulled Silero
            VAD export (`antfly inference pull onnx-community/silero-vad --tasks vad`),
            512-sample frames at 16 kHz are scored by the neural model, which
            separates speech from tones, music, and keyboard noise that the
            energy rule accepts.
        audio_context (InferenceAudioContext | Unset): How much of Whisper's 30 s window the encoder processes. `full`
            pads
            every clip to 30 s, which is what the model was trained on and gives
            the most accurate transcripts. `dynamic` trims the encoder to the
            audio actually present (plus one second), which cuts encoder time
            roughly in proportion for short clips at a small accuracy cost on
            some models. Dictation defaults to `full`; streaming sessions default
            to `dynamic` because partials re-decode short open segments many times.
        partial_interval_ms (int | Unset): Minimum new audio before the open segment is decoded again for a partial.
            Each partial is a full Whisper pass, so lower values raise decoder load. Default 2000.
        max_segment_ms (int | Unset): Continuous speech that forces a segment boundary. Default 25000.
        emit_partials (bool | Unset): Emit partial hypotheses for the open segment. Default: True.
        dictionary (list[str] | Unset): Preferred spellings for names and terms; joined into the recognizer's preceding-
            context prompt.
        transcript_prompt (str | Unset): Explicit preceding-context text for the recognizer. Overrides `dictionary`.
        ttl_seconds (int | Unset): Idle time after which the session expires. Default 300.
    """

    model: str
    language: str | Unset = UNSET
    vad: InferenceVadConfig | Unset = UNSET
    audio_context: InferenceAudioContext | Unset = UNSET
    partial_interval_ms: int | Unset = UNSET
    max_segment_ms: int | Unset = UNSET
    emit_partials: bool | Unset = True
    dictionary: list[str] | Unset = UNSET
    transcript_prompt: str | Unset = UNSET
    ttl_seconds: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        language = self.language

        vad: dict[str, Any] | Unset = UNSET
        if not isinstance(self.vad, Unset):
            vad = self.vad.to_dict()

        audio_context: str | Unset = UNSET
        if not isinstance(self.audio_context, Unset):
            audio_context = self.audio_context.value

        partial_interval_ms = self.partial_interval_ms

        max_segment_ms = self.max_segment_ms

        emit_partials = self.emit_partials

        dictionary: list[str] | Unset = UNSET
        if not isinstance(self.dictionary, Unset):
            dictionary = self.dictionary

        transcript_prompt = self.transcript_prompt

        ttl_seconds = self.ttl_seconds

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if language is not UNSET:
            field_dict["language"] = language
        if vad is not UNSET:
            field_dict["vad"] = vad
        if audio_context is not UNSET:
            field_dict["audio_context"] = audio_context
        if partial_interval_ms is not UNSET:
            field_dict["partial_interval_ms"] = partial_interval_ms
        if max_segment_ms is not UNSET:
            field_dict["max_segment_ms"] = max_segment_ms
        if emit_partials is not UNSET:
            field_dict["emit_partials"] = emit_partials
        if dictionary is not UNSET:
            field_dict["dictionary"] = dictionary
        if transcript_prompt is not UNSET:
            field_dict["transcript_prompt"] = transcript_prompt
        if ttl_seconds is not UNSET:
            field_dict["ttl_seconds"] = ttl_seconds

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_vad_config import InferenceVadConfig

        d = dict(src_dict)
        model = d.pop("model")

        language = d.pop("language", UNSET)

        _vad = d.pop("vad", UNSET)
        vad: InferenceVadConfig | Unset
        if isinstance(_vad, Unset):
            vad = UNSET
        else:
            vad = InferenceVadConfig.from_dict(_vad)

        _audio_context = d.pop("audio_context", UNSET)
        audio_context: InferenceAudioContext | Unset
        if isinstance(_audio_context, Unset):
            audio_context = UNSET
        else:
            audio_context = InferenceAudioContext(_audio_context)

        partial_interval_ms = d.pop("partial_interval_ms", UNSET)

        max_segment_ms = d.pop("max_segment_ms", UNSET)

        emit_partials = d.pop("emit_partials", UNSET)

        dictionary = cast(list[str], d.pop("dictionary", UNSET))

        transcript_prompt = d.pop("transcript_prompt", UNSET)

        ttl_seconds = d.pop("ttl_seconds", UNSET)

        inference_transcription_session_request = cls(
            model=model,
            language=language,
            vad=vad,
            audio_context=audio_context,
            partial_interval_ms=partial_interval_ms,
            max_segment_ms=max_segment_ms,
            emit_partials=emit_partials,
            dictionary=dictionary,
            transcript_prompt=transcript_prompt,
            ttl_seconds=ttl_seconds,
        )

        inference_transcription_session_request.additional_properties = d
        return inference_transcription_session_request

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
