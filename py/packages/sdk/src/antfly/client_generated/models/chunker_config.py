from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chunker_provider import ChunkerProvider
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.audio_chunk_options import AudioChunkOptions
    from ..models.chunk_embedding_target import ChunkEmbeddingTarget
    from ..models.chunk_outputs import ChunkOutputs
    from ..models.chunker_config_full_text_index import ChunkerConfigFullTextIndex
    from ..models.media_chunk_policy import MediaChunkPolicy
    from ..models.text_chunk_options import TextChunkOptions


T = TypeVar("T", bound="ChunkerConfig")


@_attrs_define
class ChunkerConfig:
    """A unified configuration for a chunking provider.

    Example:
        {'provider': 'antfly', 'model': 'fixed', 'text': {'target_tokens': 500, 'overlap_tokens': 50}}

    Attributes:
        provider (ChunkerProvider): The chunking provider to use.
        max_chunks (int | Unset): Maximum chunks per invocation. Model-aware enrichment treats this as a page size and
            resumes until source completion; zero selects the worker page size. Fixed chunking retains its per-document cap,
            with zero meaning unlimited.
        threshold (float | Unset): Confidence threshold for model-based chunking (0.0-1.0).
        text (TextChunkOptions | Unset): Options specific to text chunking.
        audio (AudioChunkOptions | Unset): Options specific to audio chunking.
        embedding (ChunkEmbeddingTarget | Unset):
        outputs (ChunkOutputs | Unset): Named independent media outputs. The named boundary model alone determines the
            shared plan; adding or changing another output preserves parent IDs. The compatibility embedding target is named
            embedding. Smaller output windows are identified by the parent ordinal and sub_index.
        policy (MediaChunkPolicy | Unset): Semantic chunk policy. Execution batch size and queue depth are deliberately
            separate.
        api_url (str | Unset): The URL of the Inference API endpoint. Can also be set via ANTFLY_INFERENCE_URL. Example:
            http://localhost:8080.
        model (str | Unset): The chunking model to use. Defaults to 'fixed' for simple token-based chunking; other
            values select a model from models/chunkers/{name}/. Successful create responses include the effective model.
            Default: 'fixed'. Example: fixed.
        store_chunks (bool | Unset): Controls whether chunk data is persisted to storage. When false (default), fixed
            chunks are generated in memory and only embeddings are stored. Model-aware streams always retain descriptor
            recipes; false omits their payloads. When true, chunk payloads are also stored. Vectors remain in the embedding
            artifact family. Default: False.
        full_text_index (ChunkerConfigFullTextIndex | Unset): Configuration for full-text indexing of chunks.
            When present (even if empty), chunk artifacts are persisted and indexed in Antfly's native full-text index,
            queryable and projectable via the document's `_chunks` field.
            When absent, chunks are generated only to drive vector embeddings and are not indexed for full-text search
            (unless `store_chunks` is also set).
    """

    provider: ChunkerProvider
    max_chunks: int | Unset = UNSET
    threshold: float | Unset = UNSET
    text: TextChunkOptions | Unset = UNSET
    audio: AudioChunkOptions | Unset = UNSET
    embedding: ChunkEmbeddingTarget | Unset = UNSET
    outputs: ChunkOutputs | Unset = UNSET
    policy: MediaChunkPolicy | Unset = UNSET
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

        embedding: dict[str, Any] | Unset = UNSET
        if not isinstance(self.embedding, Unset):
            embedding = self.embedding.to_dict()

        outputs: dict[str, Any] | Unset = UNSET
        if not isinstance(self.outputs, Unset):
            outputs = self.outputs.to_dict()

        policy: dict[str, Any] | Unset = UNSET
        if not isinstance(self.policy, Unset):
            policy = self.policy.to_dict()

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
        if embedding is not UNSET:
            field_dict["embedding"] = embedding
        if outputs is not UNSET:
            field_dict["outputs"] = outputs
        if policy is not UNSET:
            field_dict["policy"] = policy
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
        from ..models.chunk_embedding_target import ChunkEmbeddingTarget
        from ..models.chunk_outputs import ChunkOutputs
        from ..models.chunker_config_full_text_index import ChunkerConfigFullTextIndex
        from ..models.media_chunk_policy import MediaChunkPolicy
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

        _embedding = d.pop("embedding", UNSET)
        embedding: ChunkEmbeddingTarget | Unset
        if isinstance(_embedding, Unset):
            embedding = UNSET
        else:
            embedding = ChunkEmbeddingTarget.from_dict(_embedding)

        _outputs = d.pop("outputs", UNSET)
        outputs: ChunkOutputs | Unset
        if isinstance(_outputs, Unset):
            outputs = UNSET
        else:
            outputs = ChunkOutputs.from_dict(_outputs)

        _policy = d.pop("policy", UNSET)
        policy: MediaChunkPolicy | Unset
        if isinstance(_policy, Unset):
            policy = UNSET
        else:
            policy = MediaChunkPolicy.from_dict(_policy)

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
            embedding=embedding,
            outputs=outputs,
            policy=policy,
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
