from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chunker_provider import ChunkerProvider
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.audio_chunk_options import AudioChunkOptions
    from ..models.chunker_config_full_text_index import ChunkerConfigFullTextIndex
    from ..models.text_chunk_options import TextChunkOptions


T = TypeVar("T", bound="ChunkerConfig")


@_attrs_define
class ChunkerConfig:
    """A unified configuration for a chunking provider.

    Example:
        {'provider': 'antfly', 'model': 'fixed', 'text': {'target_tokens': 500, 'overlap_tokens': 50}}

    Attributes:
        provider (ChunkerProvider): The chunking provider to use.
        max_chunks (int | Unset): Maximum number of chunks to generate per document.
        threshold (float | Unset): Confidence threshold for model-based chunking (0.0-1.0).
        text (TextChunkOptions | Unset): Options specific to text chunking.
        audio (AudioChunkOptions | Unset): Options specific to audio chunking.
        api_url (str | Unset): The URL of the Inference API endpoint. Can also be set via ANTFLY_INFERENCE_URL. Example:
            http://localhost:8080.
        model (str | Unset): The chunking model to use. Defaults to 'fixed' for simple token-based chunking; other
            values select a model from models/chunkers/{name}/. Successful create responses include the effective model.
            Default: 'fixed'. Example: fixed.
        store_chunks (bool | Unset): Controls whether chunk data is persisted to storage. When false (default), chunks
            are generated in memory and only embeddings are stored. When true, both chunks and embeddings are stored.
            Default: False.
        full_text_index (ChunkerConfigFullTextIndex | Unset): Configuration for full-text indexing of chunks in Bleve.
            When present (even if empty), chunks will be stored with :cft: suffix and indexed in Bleve's _chunks field.
            When absent, chunks use :c: suffix and are only used for vector embeddings.
    """

    provider: ChunkerProvider
    max_chunks: int | Unset = UNSET
    threshold: float | Unset = UNSET
    text: TextChunkOptions | Unset = UNSET
    audio: AudioChunkOptions | Unset = UNSET
    api_url: str | Unset = UNSET
    model: str | Unset = "fixed"
    store_chunks: bool | Unset = False
    full_text_index: ChunkerConfigFullTextIndex | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        max_chunks = self.max_chunks

        threshold = self.threshold

        text: dict[str, Any] | Unset = UNSET
        if not isinstance(self.text, Unset):
            text = self.text.to_dict()

        audio: dict[str, Any] | Unset = UNSET
        if not isinstance(self.audio, Unset):
            audio = self.audio.to_dict()

        api_url = self.api_url

        model = self.model

        store_chunks = self.store_chunks

        full_text_index: dict[str, Any] | Unset = UNSET
        if not isinstance(self.full_text_index, Unset):
            full_text_index = self.full_text_index.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
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
        if model is not UNSET:
            field_dict["model"] = model
        if store_chunks is not UNSET:
            field_dict["store_chunks"] = store_chunks
        if full_text_index is not UNSET:
            field_dict["full_text_index"] = full_text_index

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.audio_chunk_options import AudioChunkOptions
        from ..models.chunker_config_full_text_index import ChunkerConfigFullTextIndex
        from ..models.text_chunk_options import TextChunkOptions

        d = dict(src_dict)
        provider = ChunkerProvider(d.pop("provider"))

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

        model = d.pop("model", UNSET)

        store_chunks = d.pop("store_chunks", UNSET)

        _full_text_index = d.pop("full_text_index", UNSET)
        full_text_index: ChunkerConfigFullTextIndex | Unset
        if isinstance(_full_text_index, Unset):
            full_text_index = UNSET
        else:
            full_text_index = ChunkerConfigFullTextIndex.from_dict(_full_text_index)

        chunker_config = cls(
            provider=provider,
            max_chunks=max_chunks,
            threshold=threshold,
            text=text,
            audio=audio,
            api_url=api_url,
            model=model,
            store_chunks=store_chunks,
            full_text_index=full_text_index,
        )

        chunker_config.additional_properties = d
        return chunker_config

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
