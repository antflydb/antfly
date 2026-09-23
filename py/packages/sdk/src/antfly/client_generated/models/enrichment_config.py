from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.enrichment_kind import EnrichmentKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.enrichment_neighbor_context_config import EnrichmentNeighborContextConfig
    from ..models.execution_policy import ExecutionPolicy
    from ..models.transcriber_enrichment_config import TranscriberEnrichmentConfig


T = TypeVar("T", bound="EnrichmentConfig")


@_attrs_define
class EnrichmentConfig:
    """Inline managed enrichment definition. Enrichments materialize generated artifacts before indexing and may target
    source rows or previously generated artifact streams.

        Attributes:
            name (str): Stable generated artifact name.
            kind (EnrichmentKind): Managed generated artifact kind.
            field (str | Unset): Source field to read from the source document or source artifact payload.
            template (str | Unset): Optional template for generated text input.
            source_artifact_name (str | Unset): Existing artifact stream this enrichment consumes. Chunk enrichments may
                consume asset artifacts; embedding enrichments may consume chunk artifacts; asset enrichments may consume other
                asset artifacts (the upstream asset's produced bytes become this producer's source, so field and template must
                be omitted and the producer must consume text: copy, generator, or extractor).
            expected_dims (int | Unset): Expected embedding dimension for embedding enrichments.
            vector_space (str | Unset): Optional stable model/token-space identifier for embedding artifacts. When omitted
                on every source, Antfly requires the effective producers to be semantically equivalent. To combine intentionally
                compatible but distinct producers, set the same identifier on every source. Explicit and implicit modes cannot
                be mixed; dimensions are always validated independently.
            chunk_size (int | Unset): Chunk size for chunk enrichments.
            chunk_overlap (int | Unset): Chunk overlap for chunk enrichments.
            chunker_json (str | Unset): Serialized chunker configuration for chunk enrichments.
            full_text_index (bool | Unset): When true on a chunk or asset enrichment, route generated text into the table's
                default full-text index. Default: False.
            content_type (str | Unset): Produced asset content type for asset enrichments.
            producer_json (str | Unset): Write-only serialized producer configuration. For managed embedding enrichments
                Antfly stores a canonical semantic producer identity here; credentials and execution policy are excluded.
            neighbor_context (EnrichmentNeighborContextConfig | Unset): Bounded sample of the document's same-shard graph
                neighbors appended to an asset producer's rendered input as a compact JSON block
                ({"neighbors":[{"edge_type":...,"direction":...,"target":...,"weight":...}]}), ordered by edge type then target
                key. A conceptualizer enrichment on an entities table can thereby ground its abstractions in adjacent facts
                ("started_by -> John Andrew Rice"). The sampled block participates in the producer's skip state, so a changed
                adjacency re-runs the producer.
            execution (ExecutionPolicy | Unset): Non-semantic execution policy for one producer or index maintenance
                operation. These fields tune how work is batched and do not change generated artifact identity.
            transcriber (TranscriberEnrichmentConfig | Unset): Speech-to-text provider for the `transcriber` enrichment
                shorthand.

                Carries the provider's STT configuration (`provider`, `model`, `api_url`, `api_key`, ...) plus the transcription
                options below. The fields are declared inline rather than composed from `STTConfig` so that a generated client
                can leave an option out: a composed schema makes a typed client serialize every field, and a
                `max_download_bytes` of zero would reject every recording.

                **Example:**
                ```yaml
                name: call_transcripts
                kind: asset
                field: recording_url
                transcriber:
                  provider: antfly
                  model: openai/whisper-base
                  language_code: en
                  timestamps: true
                ```
    """

    name: str
    kind: EnrichmentKind
    field: str | Unset = UNSET
    template: str | Unset = UNSET
    source_artifact_name: str | Unset = UNSET
    expected_dims: int | Unset = UNSET
    vector_space: str | Unset = UNSET
    chunk_size: int | Unset = UNSET
    chunk_overlap: int | Unset = UNSET
    chunker_json: str | Unset = UNSET
    full_text_index: bool | Unset = False
    content_type: str | Unset = UNSET
    producer_json: str | Unset = UNSET
    neighbor_context: EnrichmentNeighborContextConfig | Unset = UNSET
    execution: ExecutionPolicy | Unset = UNSET
    transcriber: TranscriberEnrichmentConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind = self.kind.value

        field = self.field

        template = self.template

        source_artifact_name = self.source_artifact_name

        expected_dims = self.expected_dims

        vector_space = self.vector_space

        chunk_size = self.chunk_size

        chunk_overlap = self.chunk_overlap

        chunker_json = self.chunker_json

        full_text_index = self.full_text_index

        content_type = self.content_type

        producer_json = self.producer_json

        neighbor_context: dict[str, Any] | Unset = UNSET
        if not isinstance(self.neighbor_context, Unset):
            neighbor_context = self.neighbor_context.to_dict()

        execution: dict[str, Any] | Unset = UNSET
        if not isinstance(self.execution, Unset):
            execution = self.execution.to_dict()

        transcriber: dict[str, Any] | Unset = UNSET
        if not isinstance(self.transcriber, Unset):
            transcriber = self.transcriber.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "kind": kind,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field
        if template is not UNSET:
            field_dict["template"] = template
        if source_artifact_name is not UNSET:
            field_dict["source_artifact_name"] = source_artifact_name
        if expected_dims is not UNSET:
            field_dict["expected_dims"] = expected_dims
        if vector_space is not UNSET:
            field_dict["vector_space"] = vector_space
        if chunk_size is not UNSET:
            field_dict["chunk_size"] = chunk_size
        if chunk_overlap is not UNSET:
            field_dict["chunk_overlap"] = chunk_overlap
        if chunker_json is not UNSET:
            field_dict["chunker_json"] = chunker_json
        if full_text_index is not UNSET:
            field_dict["full_text_index"] = full_text_index
        if content_type is not UNSET:
            field_dict["content_type"] = content_type
        if producer_json is not UNSET:
            field_dict["producer_json"] = producer_json
        if neighbor_context is not UNSET:
            field_dict["neighbor_context"] = neighbor_context
        if execution is not UNSET:
            field_dict["execution"] = execution
        if transcriber is not UNSET:
            field_dict["transcriber"] = transcriber

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.enrichment_neighbor_context_config import EnrichmentNeighborContextConfig
        from ..models.execution_policy import ExecutionPolicy
        from ..models.transcriber_enrichment_config import TranscriberEnrichmentConfig

        d = dict(src_dict)
        name = d.pop("name")

        kind = EnrichmentKind(d.pop("kind"))

        field = d.pop("field", UNSET)

        template = d.pop("template", UNSET)

        source_artifact_name = d.pop("source_artifact_name", UNSET)

        expected_dims = d.pop("expected_dims", UNSET)

        vector_space = d.pop("vector_space", UNSET)

        chunk_size = d.pop("chunk_size", UNSET)

        chunk_overlap = d.pop("chunk_overlap", UNSET)

        chunker_json = d.pop("chunker_json", UNSET)

        full_text_index = d.pop("full_text_index", UNSET)

        content_type = d.pop("content_type", UNSET)

        producer_json = d.pop("producer_json", UNSET)

        _neighbor_context = d.pop("neighbor_context", UNSET)
        neighbor_context: EnrichmentNeighborContextConfig | Unset
        if isinstance(_neighbor_context, Unset):
            neighbor_context = UNSET
        else:
            neighbor_context = EnrichmentNeighborContextConfig.from_dict(_neighbor_context)

        _execution = d.pop("execution", UNSET)
        execution: ExecutionPolicy | Unset
        if isinstance(_execution, Unset):
            execution = UNSET
        else:
            execution = ExecutionPolicy.from_dict(_execution)

        _transcriber = d.pop("transcriber", UNSET)
        transcriber: TranscriberEnrichmentConfig | Unset
        if isinstance(_transcriber, Unset):
            transcriber = UNSET
        else:
            transcriber = TranscriberEnrichmentConfig.from_dict(_transcriber)

        enrichment_config = cls(
            name=name,
            kind=kind,
            field=field,
            template=template,
            source_artifact_name=source_artifact_name,
            expected_dims=expected_dims,
            vector_space=vector_space,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            chunker_json=chunker_json,
            full_text_index=full_text_index,
            content_type=content_type,
            producer_json=producer_json,
            neighbor_context=neighbor_context,
            execution=execution,
            transcriber=transcriber,
        )

        enrichment_config.additional_properties = d
        return enrichment_config

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
