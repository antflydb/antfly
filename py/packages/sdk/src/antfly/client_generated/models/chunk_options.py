from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.audio_chunk_options import AudioChunkOptions
    from ..models.chunk_embedding_target import ChunkEmbeddingTarget
    from ..models.media_chunk_policy import MediaChunkPolicy
    from ..models.text_chunk_options import TextChunkOptions


T = TypeVar("T", bound="ChunkOptions")


@_attrs_define
class ChunkOptions:
    """Per-request configuration for chunking. All fields are optional - zero/omitted values use chunker defaults.

    Attributes:
        max_chunks (int | Unset): Maximum number of chunks to generate per document. Zero (the default when omitted)
            means unlimited: the document is chunked in full. Set an explicit value up to 4096 to cap output; any chunks
            beyond the cap are silently omitted, so treat a result whose chunk count equals `max_chunks` as potentially
            truncated.
        threshold (float | Unset): Confidence threshold for model-based chunking (0.0-1.0).
        text (TextChunkOptions | Unset): Options specific to text chunking.
        audio (AudioChunkOptions | Unset): Options specific to audio chunking.
        embedding (ChunkEmbeddingTarget | Unset):
        policy (MediaChunkPolicy | Unset): Semantic chunk policy. Execution batch size and queue depth are deliberately
            separate.
    """

    max_chunks: int | Unset = UNSET
    threshold: float | Unset = UNSET
    text: TextChunkOptions | Unset = UNSET
    audio: AudioChunkOptions | Unset = UNSET
    embedding: ChunkEmbeddingTarget | Unset = UNSET
    policy: MediaChunkPolicy | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        max_chunks = self.max_chunks

        threshold = self.threshold

        text: dict[str, Any] | Unset = UNSET
        if not isinstance(self.text, Unset):
            text = self.text.to_dict()

        audio: dict[str, Any] | Unset = UNSET
        if not isinstance(self.audio, Unset):
            audio = self.audio.to_dict()

        embedding: dict[str, Any] | Unset = UNSET
        if not isinstance(self.embedding, Unset):
            embedding = self.embedding.to_dict()

        policy: dict[str, Any] | Unset = UNSET
        if not isinstance(self.policy, Unset):
            policy = self.policy.to_dict()

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
        if embedding is not UNSET:
            field_dict["embedding"] = embedding
        if policy is not UNSET:
            field_dict["policy"] = policy

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.audio_chunk_options import AudioChunkOptions
        from ..models.chunk_embedding_target import ChunkEmbeddingTarget
        from ..models.media_chunk_policy import MediaChunkPolicy
        from ..models.text_chunk_options import TextChunkOptions

        d = dict(src_dict)
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

        _embedding = d.pop("embedding", UNSET)
        embedding: ChunkEmbeddingTarget | Unset
        if isinstance(_embedding, Unset):
            embedding = UNSET
        else:
            embedding = ChunkEmbeddingTarget.from_dict(_embedding)

        _policy = d.pop("policy", UNSET)
        policy: MediaChunkPolicy | Unset
        if isinstance(_policy, Unset):
            policy = UNSET
        else:
            policy = MediaChunkPolicy.from_dict(_policy)

        chunk_options = cls(
            max_chunks=max_chunks,
            threshold=threshold,
            text=text,
            audio=audio,
            embedding=embedding,
            policy=policy,
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
