"""Validated helpers for artifact-backed index configuration."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from math import isfinite
from typing import Any, Literal

MAX_ARTIFACT_SOURCES = 64


def _validate_artifacts(artifacts: Sequence[object]) -> None:
    if not artifacts:
        raise ValueError("at least one artifact source is required")
    if len(artifacts) > MAX_ARTIFACT_SOURCES:
        raise ValueError(f"at most {MAX_ARTIFACT_SOURCES} artifact sources are allowed")
    seen: set[str] = set()
    for index, artifact in enumerate(artifacts):
        if not isinstance(artifact, str) or not artifact:
            raise ValueError(f"artifacts[{index}] is required")
        if artifact in seen:
            raise ValueError(f"duplicate artifact source {artifact!r}")
        seen.add(artifact)


def artifact_index_sources(*artifacts: str) -> list[dict[str, str]]:
    """Build the shared artifact-only source shape for full-text/vector indexes."""

    _validate_artifacts(artifacts)
    return [{"artifact": artifact} for artifact in artifacts]


def artifact_full_text_index_config(name: str, *artifacts: str, mem_only: bool = False) -> dict[str, Any]:
    """Build a full-text index over generated chunk or textual asset streams."""

    if not name:
        raise ValueError("index name is required")
    result: dict[str, Any] = {
        "name": name,
        "type": "full_text",
        "sources": artifact_index_sources(*artifacts),
    }
    if mem_only:
        result["mem_only"] = True
    return result


GraphArtifactFormat = Literal["extraction_relation", "extraction_graph"]
GraphNodeModel = Literal["document", "external"]


def _clone_json_value(value: Any, path: str) -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if not isfinite(value):
            raise ValueError(f"{path} must be finite")
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} keys must be strings")
            result[key] = _clone_json_value(child, f"{path}.{key}")
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_clone_json_value(child, f"{path}[{index}]") for index, child in enumerate(value)]
    raise ValueError(f"{path} must contain only JSON values")


@dataclass(frozen=True, slots=True)
class GraphNodeMapping:
    """Optional node identifier templates for one graph artifact stream."""

    model: GraphNodeModel = "document"
    source: str | None = None
    target: str | None = None


@dataclass(frozen=True, slots=True)
class GraphEdgeMapping:
    """Optional edge templates and metadata for one graph artifact stream."""

    type: str | int | float | None = None
    weight: str | int | float | None = None
    metadata: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class GraphContextMapping:
    """Document fields explicitly exposed to graph mapping templates."""

    doc_fields: Sequence[str]


@dataclass(frozen=True, slots=True)
class GraphArtifactSource:
    """One graph artifact stream and its source-specific payload interpretation."""

    artifact: str
    path: str | None = None
    format: GraphArtifactFormat = "extraction_relation"
    mention_edge_type: str | None = None
    nodes: GraphNodeMapping | None = None
    edge: GraphEdgeMapping | None = None
    context: GraphContextMapping | None = None


def graph_index_sources(*sources: GraphArtifactSource) -> list[dict[str, Any]]:
    """Build validated graph sources without accepting unknown source fields."""

    _validate_artifacts([source.artifact for source in sources])
    result: list[dict[str, Any]] = []
    for index, source in enumerate(sources):
        if source.format not in ("extraction_relation", "extraction_graph"):
            raise ValueError(f"sources[{index}].format is invalid")
        if source.nodes is not None and source.nodes.model not in ("document", "external"):
            raise ValueError(f"sources[{index}].nodes.model is invalid")
        if source.edge is not None:
            for field_name, value in (("type", source.edge.type), ("weight", source.edge.weight)):
                if value is not None and (isinstance(value, bool) or not isinstance(value, (str, int, float))):
                    raise ValueError(f"sources[{index}].edge.{field_name} must be a string or number")
                if isinstance(value, (int, float)) and not isinstance(value, bool) and not isfinite(value):
                    raise ValueError(f"sources[{index}].edge.{field_name} must be finite")
        if source.context is not None:
            fields = list(source.context.doc_fields)
            if any(not isinstance(field, str) or not field for field in fields):
                raise ValueError(f"sources[{index}].context.doc_fields entries must be non-empty strings")
            if len(fields) != len(set(fields)):
                raise ValueError(f"sources[{index}].context.doc_fields must be unique")
        item: dict[str, Any] = {
            "artifact": source.artifact,
            "format": source.format,
        }
        if source.path is not None:
            item["path"] = source.path
        if source.mention_edge_type is not None:
            item["mention_edge_type"] = source.mention_edge_type
        if source.nodes is not None:
            item["nodes"] = {
                key: value
                for key, value in {
                    "model": source.nodes.model,
                    "source": source.nodes.source,
                    "target": source.nodes.target,
                }.items()
                if value is not None
            }
        if source.edge is not None:
            item["edge"] = {
                key: value
                for key, value in {
                    "type": source.edge.type,
                    "weight": source.edge.weight,
                    "metadata": (
                        _clone_json_value(source.edge.metadata, f"sources[{index}].edge.metadata")
                        if source.edge.metadata is not None
                        else None
                    ),
                }.items()
                if value is not None
            }
        if source.context is not None:
            item["context"] = {"doc_fields": list(source.context.doc_fields)}
        result.append(item)
    return result


@dataclass(frozen=True, slots=True)
class ArtifactEmbeddingSource:
    """One embedding enrichment and the artifact name consumed by an index."""

    artifact: str
    source_artifact: str | None = None
    field: str = "text"
    template: str | None = None


def _model_dict(value: Mapping[str, Any] | Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        result = to_dict()
        if isinstance(result, dict):
            return result
    raise TypeError("embedder must be a mapping or generated SDK model")


def artifact_embedding_index_config(
    name: str,
    *,
    sources: Sequence[ArtifactEmbeddingSource],
    embedder: Mapping[str, Any] | Any,
    dimension: int | None = None,
    sparse: bool = False,
    distance_metric: str | None = None,
    vector_space: str | None = None,
) -> dict[str, Any]:
    """Build one vector-index config plus its embedding enrichments.

    Omitting ``vector_space`` selects Antfly's automatic semantic-producer
    validation. Set it only to assert compatibility across distinct producers.
    """

    if not name:
        raise ValueError("index name is required")
    _validate_artifacts([source.artifact for source in sources])
    if sparse and dimension is not None:
        raise ValueError("dimension must be omitted for sparse embedding indexes")
    if sparse and distance_metric is not None:
        raise ValueError("distance_metric must be omitted for sparse embedding indexes")
    if dimension is not None and (not isinstance(dimension, int) or isinstance(dimension, bool) or dimension <= 0):
        raise ValueError("dimension must be a positive integer")

    embedder_config = _model_dict(embedder)
    if not isinstance(embedder_config.get("provider"), str) or not embedder_config["provider"]:
        raise ValueError("embedder.provider is required")
    if distance_metric not in (None, "l2_squared", "inner_product", "cosine"):
        raise ValueError("distance_metric is invalid")

    enrichments: list[dict[str, Any]] = []
    for index, source in enumerate(sources):
        if not source.field and not source.template:
            raise ValueError(f"sources[{index}] requires field or template")
        if source.source_artifact == "":
            raise ValueError(f"sources[{index}].source_artifact cannot be empty")
        enrichment: dict[str, Any] = {
            "name": source.artifact,
            "kind": "embedding",
        }
        if source.field:
            enrichment["field"] = source.field
        if source.template is not None:
            enrichment["template"] = source.template
        if source.source_artifact is not None:
            enrichment["source_artifact_name"] = source.source_artifact
        if dimension is not None:
            enrichment["expected_dims"] = dimension
        if vector_space is not None:
            if not vector_space:
                raise ValueError("vector_space cannot be empty when provided")
            enrichment["vector_space"] = vector_space
        enrichments.append(enrichment)

    result: dict[str, Any] = {
        "name": name,
        "type": "embeddings",
        "sources": artifact_index_sources(*(source.artifact for source in sources)),
        "enrichments": enrichments,
        "embedder": embedder_config,
    }
    if sparse:
        result["sparse"] = True
    if dimension is not None:
        result["dimension"] = dimension
    if distance_metric is not None:
        result["distance_metric"] = distance_metric
    return result
