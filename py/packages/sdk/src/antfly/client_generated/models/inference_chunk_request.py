from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chunk_embedding_target import ChunkEmbeddingTarget
    from ..models.chunk_outputs import ChunkOutputs
    from ..models.chunk_source_reference import ChunkSourceReference
    from ..models.inference_chunk_config import InferenceChunkConfig
    from ..models.media_chunk_policy import MediaChunkPolicy
    from ..models.media_content_part import MediaContentPart
    from ..models.text_content_part import TextContentPart


T = TypeVar("T", bound="InferenceChunkRequest")


@_attrs_define
class InferenceChunkRequest:
    """
    Attributes:
        input_ (ChunkSourceReference | MediaContentPart | str | TextContentPart): Input content to chunk. Supports two
            formats:
            - Text string: `"This is a long document..."`
            - ContentPart: `{"type": "media", "data": "<base64>", "mime_type": "audio/wav"}`
            - ContentPart: `{"type": "text", "text": "..."}`
        config (InferenceChunkConfig | Unset): Configuration for chunking requests to Antfly inference.
            Combines shared text options with inference-specific audio/VAD options.
        embedding (ChunkEmbeddingTarget | Unset):
        outputs (ChunkOutputs | Unset): Named independent media outputs. The named boundary model alone determines the
            shared plan; adding or changing another output preserves parent IDs. The compatibility embedding target is named
            embedding. Smaller output windows are identified by the parent ordinal and sub_index.
        allow_partial (bool | Unset): Continue enumerating descriptors after individual output failures. Applies to
            named outputs, including transcribe-only requests; overrides the legacy embedding.allow_partial option when
            present.
        policy (MediaChunkPolicy | Unset): Semantic chunk policy. Execution batch size and queue depth are deliberately
            separate.
        source_id (str | Unset): Caller source identity; immutable content is independently fingerprinted.
        source_version (str | Unset): Immutable caller version, included in continuation and plan identity.
        continuation (str | Unset): Resume the exact source and semantic plan; changed inputs are rejected.
        include_content (bool | Unset): In model-aware mode include chunk payloads in addition to mandatory descriptors.
            Default: False.
        stream (bool | Unset): In model-aware mode return NDJSON chunk events followed by a terminal coverage response.
            Default: False.
    """

    input_: ChunkSourceReference | MediaContentPart | str | TextContentPart
    config: InferenceChunkConfig | Unset = UNSET
    embedding: ChunkEmbeddingTarget | Unset = UNSET
    outputs: ChunkOutputs | Unset = UNSET
    allow_partial: bool | Unset = UNSET
    policy: MediaChunkPolicy | Unset = UNSET
    source_id: str | Unset = UNSET
    source_version: str | Unset = UNSET
    continuation: str | Unset = UNSET
    include_content: bool | Unset = False
    stream: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.chunk_source_reference import ChunkSourceReference
        from ..models.media_content_part import MediaContentPart
        from ..models.text_content_part import TextContentPart

        input_: dict[str, Any] | str
        if isinstance(self.input_, TextContentPart):
            input_ = self.input_.to_dict()
        elif isinstance(self.input_, MediaContentPart):
            input_ = self.input_.to_dict()
        elif isinstance(self.input_, ChunkSourceReference):
            input_ = self.input_.to_dict()
        else:
            input_ = self.input_

        config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.config, Unset):
            config = self.config.to_dict()

        embedding: dict[str, Any] | Unset = UNSET
        if not isinstance(self.embedding, Unset):
            embedding = self.embedding.to_dict()

        outputs: dict[str, Any] | Unset = UNSET
        if not isinstance(self.outputs, Unset):
            outputs = self.outputs.to_dict()

        allow_partial = self.allow_partial

        policy: dict[str, Any] | Unset = UNSET
        if not isinstance(self.policy, Unset):
            policy = self.policy.to_dict()

        source_id = self.source_id

        source_version = self.source_version

        continuation = self.continuation

        include_content = self.include_content

        stream = self.stream

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "input": input_,
            }
        )
        if config is not UNSET:
            field_dict["config"] = config
        if embedding is not UNSET:
            field_dict["embedding"] = embedding
        if outputs is not UNSET:
            field_dict["outputs"] = outputs
        if allow_partial is not UNSET:
            field_dict["allow_partial"] = allow_partial
        if policy is not UNSET:
            field_dict["policy"] = policy
        if source_id is not UNSET:
            field_dict["source_id"] = source_id
        if source_version is not UNSET:
            field_dict["source_version"] = source_version
        if continuation is not UNSET:
            field_dict["continuation"] = continuation
        if include_content is not UNSET:
            field_dict["include_content"] = include_content
        if stream is not UNSET:
            field_dict["stream"] = stream

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chunk_embedding_target import ChunkEmbeddingTarget
        from ..models.chunk_outputs import ChunkOutputs
        from ..models.chunk_source_reference import ChunkSourceReference
        from ..models.inference_chunk_config import InferenceChunkConfig
        from ..models.media_chunk_policy import MediaChunkPolicy
        from ..models.media_content_part import MediaContentPart
        from ..models.text_content_part import TextContentPart

        d = dict(src_dict)

        def _parse_input_(data: object) -> ChunkSourceReference | MediaContentPart | str | TextContentPart:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_inference_chunk_content_part_type_0 = TextContentPart.from_dict(data)

                return componentsschemas_inference_chunk_content_part_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_inference_chunk_content_part_type_1 = MediaContentPart.from_dict(data)

                return componentsschemas_inference_chunk_content_part_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_inference_chunk_content_part_type_2 = ChunkSourceReference.from_dict(data)

                return componentsschemas_inference_chunk_content_part_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(ChunkSourceReference | MediaContentPart | str | TextContentPart, data)

        input_ = _parse_input_(d.pop("input"))

        _config = d.pop("config", UNSET)
        config: InferenceChunkConfig | Unset
        if isinstance(_config, Unset):
            config = UNSET
        else:
            config = InferenceChunkConfig.from_dict(_config)

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

        allow_partial = d.pop("allow_partial", UNSET)

        _policy = d.pop("policy", UNSET)
        policy: MediaChunkPolicy | Unset
        if isinstance(_policy, Unset):
            policy = UNSET
        else:
            policy = MediaChunkPolicy.from_dict(_policy)

        source_id = d.pop("source_id", UNSET)

        source_version = d.pop("source_version", UNSET)

        continuation = d.pop("continuation", UNSET)

        include_content = d.pop("include_content", UNSET)

        stream = d.pop("stream", UNSET)

        inference_chunk_request = cls(
            input_=input_,
            config=config,
            embedding=embedding,
            outputs=outputs,
            allow_partial=allow_partial,
            policy=policy,
            source_id=source_id,
            source_version=source_version,
            continuation=continuation,
            include_content=include_content,
            stream=stream,
        )

        inference_chunk_request.additional_properties = d
        return inference_chunk_request

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
