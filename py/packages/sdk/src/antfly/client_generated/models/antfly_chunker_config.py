from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.audio_chunk_options import AudioChunkOptions
    from ..models.text_chunk_options import TextChunkOptions


T = TypeVar("T", bound="AntflyChunkerConfig")


@_attrs_define
class AntflyChunkerConfig:
    r"""Configuration for the Antfly inference chunking provider.

    Antfly inference is a centralized HTTP service that provides chunking with multi-tier caching.
    The model name maps to ONNX model directory names (similar to how Ollama works).

    **Chunking Models:**
    - fixed: Simple fixed-size chunking by token count (built-in, no ONNX required)
    - Any other name will attempt to load from models/chunkers/{name}/ directory

    **Caching:**
    - L1: Memory cache with 2-minute TTL
    - L2: Persistent Pebble database
    - Singleflight deduplication for concurrent identical requests

        Example:
            {'provider': 'antfly', 'api_url': 'http://localhost:8080', 'model': 'fixed', 'max_chunks': 50, 'text':
                {'target_tokens': 500, 'overlap_tokens': 50, 'separator': '\n\n'}}

        Attributes:
            model (str): The chunking model to use. Either 'fixed' for simple token-based chunking, or a model name from
                models/chunkers/{name}/. Default: 'fixed'. Example: fixed.
            max_chunks (int | Unset): Maximum number of chunks to generate per document.
            threshold (float | Unset): Confidence threshold for model-based chunking (0.0-1.0).
            text (TextChunkOptions | Unset): Options specific to text chunking.
            audio (AudioChunkOptions | Unset): Options specific to audio chunking.
            api_url (str | Unset): The URL of the Inference API endpoint (e.g., 'http://localhost:8080'). Can also be set
                via ANTFLY_INFERENCE_URL environment variable. Example: http://localhost:8080.
    """

    model: str = "fixed"
    max_chunks: int | Unset = UNSET
    threshold: float | Unset = UNSET
    text: TextChunkOptions | Unset = UNSET
    audio: AudioChunkOptions | Unset = UNSET
    api_url: str | Unset = UNSET
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

        api_url = self.api_url

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if max_chunks is not UNSET:
            field_dict["max_chunks"] = max_chunks
        if threshold is not UNSET:
            field_dict["threshold"] = threshold
        if text is not UNSET:
            field_dict["text"] = text
        if audio is not UNSET:
            field_dict["audio"] = audio
        if api_url is not UNSET:
            field_dict["api_url"] = api_url

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.audio_chunk_options import AudioChunkOptions
        from ..models.text_chunk_options import TextChunkOptions

        d = dict(src_dict)
        model = d.pop("model")

        max_chunks = d.pop("max_chunks", UNSET)

        threshold = d.pop("threshold", UNSET)

        _text = d.pop("text", UNSET)
        text: TextChunkOptions | Unset
        if isinstance(_text, Unset):
            text = UNSET
        else:
            text = TextChunkOptions.from_dict(_text)

        _audio = d.pop("audio", UNSET)
        audio: AudioChunkOptions | Unset
        if isinstance(_audio, Unset):
            audio = UNSET
        else:
            audio = AudioChunkOptions.from_dict(_audio)

        api_url = d.pop("api_url", UNSET)

        antfly_chunker_config = cls(
            model=model,
            max_chunks=max_chunks,
            threshold=threshold,
            text=text,
            audio=audio,
            api_url=api_url,
        )

        antfly_chunker_config.additional_properties = d
        return antfly_chunker_config

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
