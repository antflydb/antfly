"""Validated helpers for artifact-backed index configuration."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from math import isfinite
from typing import Any, Literal

MAX_ARTIFACT_SOURCES = 64
_GRAPH_ARTIFACT_PATH = re.compile(r"^(\$|\$\.[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)*(\[\*\])?)?$")
_GRAPH_MATERIALIZED_SOURCE = re.compile(r"^\{\{\s*(?:_doc\.key|_artifact\.value(?:\.[A-Za-z0-9_]+)*)\s*\}\}$")


def _relationship_field_active(value: Any) -> bool:
    return value is not None and value is not False


def validate_create_index_request_relationships(config: Mapping[str, Any]) -> None:
    """Validate the cross-field relationships published by the OpenAPI contract."""

    if not isinstance(config, Mapping):
        raise TypeError("index config must be an object")

    index_type = config.get("type")
    has_sources = _relationship_field_active(config.get("sources"))
    if index_type == "full_text" and has_sources and _relationship_field_active(config.get("artifact_name")):
        raise ValueError("index sources cannot be combined with artifact_name")
    if index_type == "graph" and has_sources and _relationship_field_active(config.get("source")):
        raise ValueError("index sources cannot be combined with source")
    if index_type != "embeddings":
        return

    if has_sources:
        for field in (
            "external",
            "field",
            "template",
            "chunker",
            "embedding_name",
            "source_artifact_name",
        ):
            if _relationship_field_active(config.get(field)):
                raise ValueError(f"index sources cannot be combined with {field}")
    if _relationship_field_active(config.get("source_artifact_name")) and not _relationship_field_active(
        config.get("embedding_name")
    ):
        raise ValueError("embedding source_artifact_name requires a non-empty embedding_name")
    embedding_name = config.get("embedding_name")
    source_artifact_name = config.get("source_artifact_name")
    enrichments = config.get("enrichments")
    if isinstance(embedding_name, str) and isinstance(source_artifact_name, str) and isinstance(enrichments, list):
        enrichment = next(
            (
                candidate
                for candidate in enrichments
                if isinstance(candidate, Mapping)
                and candidate.get("kind") == "embedding"
                and candidate.get("name") == embedding_name
            ),
            None,
        )
        if enrichment is not None and enrichment.get("source_artifact_name") != source_artifact_name:
            raise ValueError("embedding source_artifact_name must match the authoritative embedding enrichment")


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


@dataclass(frozen=True, slots=True)
class FullTextArtifactSource:
    """One textual artifact stream and its optional source-local projection."""

    artifact: str
    field: str | None = None


def _full_text_sources(sources: Sequence[FullTextArtifactSource]) -> list[dict[str, str]]:
    for index, source in enumerate(sources):
        if not isinstance(source, FullTextArtifactSource):
            raise TypeError(f"sources[{index}] must be a FullTextArtifactSource")
    _validate_artifacts([source.artifact for source in sources])
    result: list[dict[str, str]] = []
    for index, source in enumerate(sources):
        item = {"artifact": source.artifact}
        if source.field is not None:
            field = source.field.strip()
            if not field:
                raise ValueError(f"sources[{index}].field must not be empty")
            item["field"] = field
        result.append(item)
    return result


def artifact_full_text_index_config(
    name: str,
    *artifacts: str,
    sources: Sequence[FullTextArtifactSource] | None = None,
    field: str | None = None,
    mem_only: bool = False,
) -> dict[str, Any]:
    """Build a full-text index with shared or source-local content fields."""

    if not name:
        raise ValueError("index name is required")
    if artifacts and sources is not None:
        raise ValueError("artifacts and sources are mutually exclusive")
    result: dict[str, Any] = {
        "name": name,
        "type": "full_text",
        "sources": _full_text_sources(sources) if sources is not None else artifact_index_sources(*artifacts),
    }
    if field is not None:
        field = field.strip()
        if not field:
            raise ValueError("field must not be empty")
        result["field"] = field
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
    target: str | int | float | None = None


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
        if source.path is not None and _GRAPH_ARTIFACT_PATH.fullmatch(source.path) is None:
            raise ValueError(f"sources[{index}].path must be $, a dot-separated field path, or end in [*]")
        if source.format not in ("extraction_relation", "extraction_graph"):
            raise ValueError(f"sources[{index}].format is invalid")
        if source.nodes is not None and source.nodes.model not in ("document", "external"):
            raise ValueError(f"sources[{index}].nodes.model is invalid")
        if source.nodes is not None:
            if source.nodes.source is not None and (
                not isinstance(source.nodes.source, str)
                or _GRAPH_MATERIALIZED_SOURCE.fullmatch(source.nodes.source) is None
            ):
                raise ValueError(f"sources[{index}].nodes.source must use _doc.key or _artifact.value")
            target = source.nodes.target
            if target is not None and (isinstance(target, bool) or not isinstance(target, (str, int, float))):
                raise ValueError(f"sources[{index}].nodes.target must be a string or number")
            if isinstance(target, (int, float)) and not isinstance(target, bool) and not isfinite(target):
                raise ValueError(f"sources[{index}].nodes.target must be finite")
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
) -> dict[str, Any]:
    """Build one vector index plus same-producer embedding enrichments."""

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
        field = "" if source.template else (source.field or "text")
        if source.source_artifact == "":
            raise ValueError(f"sources[{index}].source_artifact cannot be empty")
        enrichment: dict[str, Any] = {
            "name": source.artifact,
            "kind": "embedding",
        }
        if field:
            enrichment["field"] = field
        if source.template is not None:
            enrichment["template"] = source.template
        if source.source_artifact is not None:
            enrichment["source_artifact_name"] = source.source_artifact
        if dimension is not None:
            enrichment["expected_dims"] = dimension
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
