from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_transcription_audio_format import InferenceTranscriptionAudioFormat
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceTranscriptionAudioAppend")


@_attrs_define
class InferenceTranscriptionAudioAppend:
    """
    Attributes:
        audio (str | Unset): Base64 audio chunk. Optional when `commit` is true.
        format_ (InferenceTranscriptionAudioFormat | Unset):
        sample_rate (int | Unset): Sample rate of raw `pcm16` / `pcm_f32` chunks. Default 16000. Ignored for containers.
        commit (bool | Unset): Finalize buffered speech even without trailing silence. Default: False.
    """

    audio: str | Unset = UNSET
    format_: InferenceTranscriptionAudioFormat | Unset = UNSET
    sample_rate: int | Unset = UNSET
    commit: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        audio = self.audio

        format_: str | Unset = UNSET
        if not isinstance(self.format_, Unset):
            format_ = self.format_.value

        sample_rate = self.sample_rate

        commit = self.commit

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if audio is not UNSET:
            field_dict["audio"] = audio
        if format_ is not UNSET:
            field_dict["format"] = format_
        if sample_rate is not UNSET:
            field_dict["sample_rate"] = sample_rate
        if commit is not UNSET:
            field_dict["commit"] = commit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        audio = d.pop("audio", UNSET)

        _format_ = d.pop("format", UNSET)
        format_: InferenceTranscriptionAudioFormat | Unset
        if isinstance(_format_, Unset):
            format_ = UNSET
        else:
            format_ = InferenceTranscriptionAudioFormat(_format_)

        sample_rate = d.pop("sample_rate", UNSET)

        commit = d.pop("commit", UNSET)

        inference_transcription_audio_append = cls(
            audio=audio,
            format_=format_,
            sample_rate=sample_rate,
            commit=commit,
        )

        inference_transcription_audio_append.additional_properties = d
        return inference_transcription_audio_append

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
