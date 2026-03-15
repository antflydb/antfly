from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.audio_chunk_options import AudioChunkOptions
    from ..models.text_chunk_options import TextChunkOptions


T = TypeVar("T", bound="ChunkOptions")


@_attrs_define
class ChunkOptions:
    """Per-request configuration for chunking. All fields are optional - zero/omitted values use chunker defaults.

    Attributes:
        max_chunks (Union[Unset, int]): Maximum number of chunks to generate per document.
        threshold (Union[Unset, float]): Confidence threshold for model-based chunking (0.0-1.0).
        text (Union[Unset, TextChunkOptions]): Options specific to text chunking.
        audio (Union[Unset, AudioChunkOptions]): Options specific to audio chunking.
    """

    max_chunks: Union[Unset, int] = UNSET
    threshold: Union[Unset, float] = UNSET
    text: Union[Unset, "TextChunkOptions"] = UNSET
    audio: Union[Unset, "AudioChunkOptions"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        max_chunks = self.max_chunks

        threshold = self.threshold

        text: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.text, Unset):
            text = self.text.to_dict()

        audio: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.audio, Unset):
            audio = self.audio.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
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
        from ..models.audio_chunk_options import AudioChunkOptions
        from ..models.text_chunk_options import TextChunkOptions

        d = dict(src_dict)
        max_chunks = d.pop("max_chunks", UNSET)

        threshold = d.pop("threshold", UNSET)

        _text = d.pop("text", UNSET)
        text: Union[Unset, TextChunkOptions]
        if isinstance(_text, Unset):
            text = UNSET
        else:
            text = TextChunkOptions.from_dict(_text)

        _audio = d.pop("audio", UNSET)
        audio: Union[Unset, AudioChunkOptions]
        if isinstance(_audio, Unset):
            audio = UNSET
        else:
            audio = AudioChunkOptions.from_dict(_audio)

        chunk_options = cls(
            max_chunks=max_chunks,
            threshold=threshold,
            text=text,
            audio=audio,
        )

        chunk_options.additional_properties = d
        return chunk_options

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
