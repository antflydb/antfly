from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_audio_chunk_config import InferenceAudioChunkConfig
    from ..models.inference_text_chunk_options import InferenceTextChunkOptions


T = TypeVar("T", bound="InferenceChunkConfig")


@_attrs_define
class InferenceChunkConfig:
    """Configuration for chunking requests to Inference API.
    Combines shared text options with inference-specific audio/VAD options.

        Attributes:
            model (str | Unset): The chunking model to use. Either 'fixed' for simple token-based chunking, or a model name
                from models/chunkers/{name}/. Default: 'fixed'. Example: fixed.
            max_chunks (int | Unset): Maximum number of chunks to generate per document.
            threshold (float | Unset): Confidence threshold for model-based chunking (0.0-1.0). Used by ONNX text models and
                VAD audio models.
            text (InferenceTextChunkOptions | Unset): Options specific to text chunking.
            audio (InferenceAudioChunkConfig | Unset): Audio chunking configuration for inference, including VAD options.
    """

    model: str | Unset = "fixed"
    max_chunks: int | Unset = UNSET
    threshold: float | Unset = UNSET
    text: InferenceTextChunkOptions | Unset = UNSET
    audio: InferenceAudioChunkConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        max_chunks = self.max_chunks

        threshold = self.threshold

        text: dict[str, Any] | Unset = UNSET
        if not isinstance(self.text, Unset):
            text = self.text.to_dict()

        audio: dict[str, Any] | Unset = UNSET
        if not isinstance(self.audio, Unset):
            audio = self.audio.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if model is not UNSET:
            field_dict["model"] = model
        if max_chunks is not UNSET:
            field_dict["max_chunks"] = max_chunks
        if threshold is not UNSET:
            field_dict["threshold"] = threshold
        if text is not UNSET:
            field_dict["text"] = text
        if audio is not UNSET:
            field_dict["audio"] = audio

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_audio_chunk_config import InferenceAudioChunkConfig
        from ..models.inference_text_chunk_options import InferenceTextChunkOptions

        d = dict(src_dict)
        model = d.pop("model", UNSET)

        max_chunks = d.pop("max_chunks", UNSET)

        threshold = d.pop("threshold", UNSET)

        _text = d.pop("text", UNSET)
        text: InferenceTextChunkOptions | Unset
        if isinstance(_text, Unset):
            text = UNSET
        else:
            text = InferenceTextChunkOptions.from_dict(_text)

        _audio = d.pop("audio", UNSET)
        audio: InferenceAudioChunkConfig | Unset
        if isinstance(_audio, Unset):
            audio = UNSET
        else:
            audio = InferenceAudioChunkConfig.from_dict(_audio)

        inference_chunk_config = cls(
            model=model,
            max_chunks=max_chunks,
            threshold=threshold,
            text=text,
            audio=audio,
        )

        inference_chunk_config.additional_properties = d
        return inference_chunk_config

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
