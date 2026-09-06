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
    from ..models.created_provider_config import CreatedProviderConfig
    from ..models.index_execution_config import IndexExecutionConfig


T = TypeVar("T", bound="CreatedEmbeddingsIndexConfig")


@_attrs_define
class CreatedEmbeddingsIndexConfig:
    """Credential-free normalized embeddings configuration returned after creation. Single-source v0.2 fields are preserved
    alongside canonical sources for read compatibility.

        Attributes:
            publication_policy (IndexPublicationPolicy | Unset): Publication behavior for a managed embeddings index.
                `progressive` makes a safely checkpointed active generation queryable before initial source coverage is
                complete. `atomic` keeps a new generation unavailable until complete validation and activation.
            coverage_policy (DerivedCoveragePolicy | Unset): How generation-scoped source outcomes determine derived-index
                completeness.
            external (bool | Unset):  Default: False.
            sparse (bool | Unset):  Default: False.
            dimension (int | Unset):
            field (str | Unset):
            sources (list[ArtifactIndexSource] | Unset): Embedding artifact streams indexed together as independent vector
                members.
            embedding_name (str | Unset): Released v0.2 single-source read field, preserved when that request form created
                the index. Canonical source identity is also returned through sources.
            source_artifact_name (str | Unset): Deprecated v0.2 descriptive source read field, preserved when supplied with
                embedding_name. The matching enrichment is authoritative.
            template (str | Unset):
            distance_metric (DistanceMetric | Unset): Distance metric for the vector index (dense only). Use "cosine" for
                models trained with cosine similarity (e.g. CLIP, OpenAI). Use "inner_product" for models trained with dot
                product similarity. Use "l2_squared" for models trained with Euclidean distance. The default is "l2_squared".
            mem_only (bool | Unset):
            embedder (CreatedProviderConfig | Unset): Credential-free provider configuration returned after index creation.
                Only non-secret provider settings are represented.
            chunker (ChunkerConfig | Unset): A unified configuration for a chunking provider. Example: {'provider':
                'antfly', 'model': 'fixed', 'text': {'target_tokens': 500, 'overlap_tokens': 50}}.
            top_k (int | Unset):  Default: 10.
            min_weight (float | Unset):  Default: 0.0.
            chunk_size (int | Unset):  Default: 1024.
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
    embedder: CreatedProviderConfig | Unset = UNSET
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
        from ..models.created_provider_config import CreatedProviderConfig
        from ..models.index_execution_config import IndexExecutionConfig

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
        embedder: CreatedProviderConfig | Unset
        if isinstance(_embedder, Unset):
            embedder = UNSET
        else:
            embedder = CreatedProviderConfig.from_dict(_embedder)

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

        created_embeddings_index_config = cls(
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

        created_embeddings_index_config.additional_properties = d
        return created_embeddings_index_config

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
