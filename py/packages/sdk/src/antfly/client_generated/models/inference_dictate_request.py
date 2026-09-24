from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_audio_context import InferenceAudioContext
from ..models.inference_dictation_style import InferenceDictationStyle
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_vad_config import InferenceVadConfig


T = TypeVar("T", bound="InferenceDictateRequest")


@_attrs_define
class InferenceDictateRequest:
    """
    Attributes:
        model (str): Transcriber model from models_dir/transcribers/. Example: openai/whisper-tiny.
        audio (str): Base64-encoded audio clip (WAV, Opus, MP3, FLAC, etc.). Clips longer than 30 s are transcribed in
            windows.
        language (str | Unset): Force the transcript language (ISO 639-1). Omit for automatic detection. Example: en.
        cleanup_model (str | Unset): Generator model from models_dir/generators/ that rewrites the transcript. Omit to
            return the raw transcript. Example: ggml-org/gemma-4-E4B-it-GGUF.
        style (InferenceDictationStyle | Unset): How the cleanup pass rewrites the transcript. `clean` removes fillers
            and fixes punctuation while keeping the speaker's wording; `formal`
            and `casual` also adjust register; `verbatim` skips the generator and
            returns the raw transcript.
        dictionary (list[str] | Unset): Preferred spellings for names and terms the recognizer tends to miss.
        context (str | Unset): Where the text will be inserted, for example "email to a customer". Steers tone and
            formatting.
        instructions (str | Unset): Extra cleanup instructions appended to the built-in rules.
        transcript_prompt (str | Unset): Text the recognizer treats as preceding context, so it prefers these spellings
            and this style. Defaults to the dictionary entries joined by commas.
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
        stream (bool | Unset): Stream the response as Server-Sent Events. Default: False.
        max_tokens (int | Unset): Output budget for the cleanup pass. Defaults to about twice the transcript length.
    """

    model: str
    audio: str
    language: str | Unset = UNSET
    cleanup_model: str | Unset = UNSET
    style: InferenceDictationStyle | Unset = UNSET
    dictionary: list[str] | Unset = UNSET
    context: str | Unset = UNSET
    instructions: str | Unset = UNSET
    transcript_prompt: str | Unset = UNSET
    vad: InferenceVadConfig | Unset = UNSET
    audio_context: InferenceAudioContext | Unset = UNSET
    stream: bool | Unset = False
    max_tokens: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        audio = self.audio

        language = self.language

        cleanup_model = self.cleanup_model

        style: str | Unset = UNSET
        if not isinstance(self.style, Unset):
            style = self.style.value

        dictionary: list[str] | Unset = UNSET
        if not isinstance(self.dictionary, Unset):
            dictionary = self.dictionary

        context = self.context

        instructions = self.instructions

        transcript_prompt = self.transcript_prompt

        vad: dict[str, Any] | Unset = UNSET
        if not isinstance(self.vad, Unset):
            vad = self.vad.to_dict()

        audio_context: str | Unset = UNSET
        if not isinstance(self.audio_context, Unset):
            audio_context = self.audio_context.value

        stream = self.stream

        max_tokens = self.max_tokens

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "audio": audio,
            }
        )
        if language is not UNSET:
            field_dict["language"] = language
        if cleanup_model is not UNSET:
            field_dict["cleanup_model"] = cleanup_model
        if style is not UNSET:
            field_dict["style"] = style
        if dictionary is not UNSET:
            field_dict["dictionary"] = dictionary
        if context is not UNSET:
            field_dict["context"] = context
        if instructions is not UNSET:
            field_dict["instructions"] = instructions
        if transcript_prompt is not UNSET:
            field_dict["transcript_prompt"] = transcript_prompt
        if vad is not UNSET:
            field_dict["vad"] = vad
        if audio_context is not UNSET:
            field_dict["audio_context"] = audio_context
        if stream is not UNSET:
            field_dict["stream"] = stream
        if max_tokens is not UNSET:
            field_dict["max_tokens"] = max_tokens

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_vad_config import InferenceVadConfig

        d = dict(src_dict)
        model = d.pop("model")

        audio = d.pop("audio")

        language = d.pop("language", UNSET)

        cleanup_model = d.pop("cleanup_model", UNSET)

        _style = d.pop("style", UNSET)
        style: InferenceDictationStyle | Unset
        if isinstance(_style, Unset):
            style = UNSET
        else:
            style = InferenceDictationStyle(_style)

        dictionary = cast(list[str], d.pop("dictionary", UNSET))

        context = d.pop("context", UNSET)

        instructions = d.pop("instructions", UNSET)

        transcript_prompt = d.pop("transcript_prompt", UNSET)

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

        stream = d.pop("stream", UNSET)

        max_tokens = d.pop("max_tokens", UNSET)

        inference_dictate_request = cls(
            model=model,
            audio=audio,
            language=language,
            cleanup_model=cleanup_model,
            style=style,
            dictionary=dictionary,
            context=context,
            instructions=instructions,
            transcript_prompt=transcript_prompt,
            vad=vad,
            audio_context=audio_context,
            stream=stream,
            max_tokens=max_tokens,
        )

        inference_dictate_request.additional_properties = d
        return inference_dictate_request

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
