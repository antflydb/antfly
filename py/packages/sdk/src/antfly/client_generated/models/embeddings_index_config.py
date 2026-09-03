from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.derived_coverage_policy import DerivedCoveragePolicy
from ..models.distance_metric import DistanceMetric
from ..models.index_publication_policy import IndexPublicationPolicy
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.artifact_index_source import ArtifactIndexSource
    from ..models.chunker_config import ChunkerConfig
    from ..models.index_execution_config import IndexExecutionConfig
    from ..models.managed_embedder_config import ManagedEmbedderConfig


T = TypeVar("T", bound="EmbeddingsIndexConfig")


@_attrs_define
class EmbeddingsIndexConfig:
    """Unified configuration for embeddings indexes. When sparse is true, creates a sparse vector index (SPLADE inverted
    index). When sparse is false (default), creates a dense HBC vector index. For dense indexes, dimension can be
    omitted if an embedder is configured — it will be auto-detected.

        Attributes:
            publication_policy (IndexPublicationPolicy | Unset): Publication behavior for a managed embeddings index.
                `progressive` makes a safely checkpointed active generation queryable before initial source coverage is
                complete. `atomic` keeps a new generation unavailable until complete validation and activation.
            coverage_policy (DerivedCoveragePolicy | Unset): How generation-scoped source outcomes determine derived-index
                completeness.
            external (bool | Unset): When true, embeddings are supplied externally via _embeddings and the index does not
                derive prompts from a field or template. Default: False.
            sparse (bool | Unset): When true, creates a sparse (SPLADE) inverted index. When false (default), creates a
                dense HBC vector index. Default: False.
            dimension (int | Unset): Vector dimension for dense indexes. Required for external dense indexes. Can be omitted
                for managed dense indexes when an embedder is configured (auto-detected via probe). Ignored for sparse indexes.
            field (str | Unset): Field to extract embeddings from (managed indexes only; not allowed when external=true)
            sources (list[ArtifactIndexSource] | Unset): Embedding artifact streams indexed together. Each artifact record
                is an independent vector member identified by (artifact name, source key). All sources must use the same dense
                vector space or sparse token space. Not allowed with external, field, template, chunker, embedding_name, or
                source_artifact_name. Requires index_capabilities.artifact_sources=true and is rejected by serverless
                deployments.
            embedding_name (str | Unset): Released v0.2 single-source alternative request form. Mutually exclusive with
                sources. Required when source_artifact_name is set. Responses also expose canonical sources while preserving
                these fields. Requires index_capabilities.artifact_sources=true and is rejected by serverless deployments.
            source_artifact_name (str | Unset): Deprecated v0.2 descriptive field. When supplied for compatibility,
                embedding_name is required and this value must exactly match the source_artifact_name on the authoritative
                embedding enrichment. New clients should declare the relationship only on that enrichment.
            template (str | Unset): Handlebars template for generating prompts (managed indexes only; not allowed when
                external=true). See https://handlebarsjs.com/guide/ for more information. Example: Hello, {{#if (eq Name
                "John")}}Johnathan{{else}}{{Name}}{{/if}}! You are {{Age}} years old..
            distance_metric (DistanceMetric | Unset): Distance metric for the vector index (dense only). Use "cosine" for
                models trained with cosine similarity (e.g. CLIP, OpenAI). Use "inner_product" for models trained with dot
                product similarity. Use "l2_squared" for models trained with Euclidean distance. The default is "l2_squared".
            mem_only (bool | Unset): Whether to use in-memory only storage (dense only)
            embedder (ManagedEmbedderConfig | Unset): Embedding provider configuration accepted by managed index creation.
            chunker (ChunkerConfig | Unset): A unified configuration for a chunking provider. Example: {'provider':
                'antfly', 'model': 'fixed', 'text': {'target_tokens': 500, 'overlap_tokens': 50}}.
            top_k (int | Unset): Default number of results to return from search (sparse only) Default: 10.
            min_weight (float | Unset): Minimum weight threshold for sparse vector entries (sparse only) Default: 0.0.
            chunk_size (int | Unset): Number of documents per posting list chunk (sparse only) Default: 1024.
            execution (IndexExecutionConfig | Unset): Namespaced execution policy for managed index shorthand. Only
                namespaces with runtime effects are accepted.
    """

    publication_policy: IndexPublicationPolicy | Unset = UNSET
    coverage_policy: DerivedCoveragePolicy | Unset = UNSET
    external: bool | Unset = False
    sparse: bool | Unset = False
    dimension: int | Unset = UNSET
    field: str | Unset = UNSET
    sources: list[ArtifactIndexSource] | Unset = UNSET
    embedding_name: str | Unset = UNSET
    source_artifact_name: str | Unset = UNSET
    template: str | Unset = UNSET
    distance_metric: DistanceMetric | Unset = UNSET
    mem_only: bool | Unset = UNSET
    embedder: ManagedEmbedderConfig | Unset = UNSET
    chunker: ChunkerConfig | Unset = UNSET
    top_k: int | Unset = 10
    min_weight: float | Unset = 0.0
    chunk_size: int | Unset = 1024
    execution: IndexExecutionConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        publication_policy: str | Unset = UNSET
        if not isinstance(self.publication_policy, Unset):
            publication_policy = self.publication_policy.value

        coverage_policy: str | Unset = UNSET
        if not isinstance(self.coverage_policy, Unset):
            coverage_policy = self.coverage_policy.value

        external = self.external

        sparse = self.sparse

        dimension = self.dimension

        field = self.field

        sources: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.sources, Unset):
            sources = []
            for sources_item_data in self.sources:
                sources_item = sources_item_data.to_dict()
                sources.append(sources_item)

        embedding_name = self.embedding_name

        source_artifact_name = self.source_artifact_name

        template = self.template

        distance_metric: str | Unset = UNSET
        if not isinstance(self.distance_metric, Unset):
            distance_metric = self.distance_metric.value

        mem_only = self.mem_only

        embedder: dict[str, Any] | Unset = UNSET
        if not isinstance(self.embedder, Unset):
            embedder = self.embedder.to_dict()

        chunker: dict[str, Any] | Unset = UNSET
        if not isinstance(self.chunker, Unset):
            chunker = self.chunker.to_dict()

        top_k = self.top_k

        min_weight = self.min_weight

        chunk_size = self.chunk_size

        execution: dict[str, Any] | Unset = UNSET
        if not isinstance(self.execution, Unset):
            execution = self.execution.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if publication_policy is not UNSET:
            field_dict["publication_policy"] = publication_policy
        if coverage_policy is not UNSET:
            field_dict["coverage_policy"] = coverage_policy
        if external is not UNSET:
            field_dict["external"] = external
        if sparse is not UNSET:
            field_dict["sparse"] = sparse
        if dimension is not UNSET:
            field_dict["dimension"] = dimension
        if field is not UNSET:
            field_dict["field"] = field
        if sources is not UNSET:
            field_dict["sources"] = sources
        if embedding_name is not UNSET:
            field_dict["embedding_name"] = embedding_name
        if source_artifact_name is not UNSET:
            field_dict["source_artifact_name"] = source_artifact_name
        if template is not UNSET:
            field_dict["template"] = template
        if distance_metric is not UNSET:
            field_dict["distance_metric"] = distance_metric
        if mem_only is not UNSET:
            field_dict["mem_only"] = mem_only
        if embedder is not UNSET:
            field_dict["embedder"] = embedder
        if chunker is not UNSET:
            field_dict["chunker"] = chunker
        if top_k is not UNSET:
            field_dict["top_k"] = top_k
        if min_weight is not UNSET:
            field_dict["min_weight"] = min_weight
        if chunk_size is not UNSET:
            field_dict["chunk_size"] = chunk_size
        if execution is not UNSET:
            field_dict["execution"] = execution

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.artifact_index_source import ArtifactIndexSource
        from ..models.chunker_config import ChunkerConfig
        from ..models.index_execution_config import IndexExecutionConfig
        from ..models.managed_embedder_config import ManagedEmbedderConfig

        d = dict(src_dict)
        _publication_policy = d.pop("publication_policy", UNSET)
        publication_policy: IndexPublicationPolicy | Unset
        if isinstance(_publication_policy, Unset):
            publication_policy = UNSET
        else:
            publication_policy = IndexPublicationPolicy(_publication_policy)

        _coverage_policy = d.pop("coverage_policy", UNSET)
        coverage_policy: DerivedCoveragePolicy | Unset
        if isinstance(_coverage_policy, Unset):
            coverage_policy = UNSET
        else:
            coverage_policy = DerivedCoveragePolicy(_coverage_policy)

        external = d.pop("external", UNSET)

        sparse = d.pop("sparse", UNSET)

        dimension = d.pop("dimension", UNSET)

        field = d.pop("field", UNSET)

        _sources = d.pop("sources", UNSET)
        sources: list[ArtifactIndexSource] | Unset = UNSET
        if _sources is not UNSET:
            sources = []
            for sources_item_data in _sources:
                sources_item = ArtifactIndexSource.from_dict(sources_item_data)

                sources.append(sources_item)

        embedding_name = d.pop("embedding_name", UNSET)

        source_artifact_name = d.pop("source_artifact_name", UNSET)

        template = d.pop("template", UNSET)

        _distance_metric = d.pop("distance_metric", UNSET)
        distance_metric: DistanceMetric | Unset
        if isinstance(_distance_metric, Unset):
            distance_metric = UNSET
        else:
            distance_metric = DistanceMetric(_distance_metric)

        mem_only = d.pop("mem_only", UNSET)

        _embedder = d.pop("embedder", UNSET)
        embedder: ManagedEmbedderConfig | Unset
        if isinstance(_embedder, Unset):
            embedder = UNSET
        else:
            embedder = ManagedEmbedderConfig.from_dict(_embedder)

        _chunker = d.pop("chunker", UNSET)
        chunker: ChunkerConfig | Unset
        if isinstance(_chunker, Unset):
            chunker = UNSET
        else:
            chunker = ChunkerConfig.from_dict(_chunker)

        top_k = d.pop("top_k", UNSET)

        min_weight = d.pop("min_weight", UNSET)

        chunk_size = d.pop("chunk_size", UNSET)

        _execution = d.pop("execution", UNSET)
        execution: IndexExecutionConfig | Unset
        if isinstance(_execution, Unset):
            execution = UNSET
        else:
            execution = IndexExecutionConfig.from_dict(_execution)

        embeddings_index_config = cls(
            publication_policy=publication_policy,
            coverage_policy=coverage_policy,
            external=external,
            sparse=sparse,
            dimension=dimension,
            field=field,
            sources=sources,
            embedding_name=embedding_name,
            source_artifact_name=source_artifact_name,
            template=template,
            distance_metric=distance_metric,
            mem_only=mem_only,
            embedder=embedder,
            chunker=chunker,
            top_k=top_k,
            min_weight=min_weight,
            chunk_size=chunk_size,
            execution=execution,
        )

        embeddings_index_config.additional_properties = d
        return embeddings_index_config

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
