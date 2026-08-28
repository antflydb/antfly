import pytest

from antfly import (
    ArtifactEmbeddingSource,
    FullTextArtifactSource,
    GraphArtifactSource,
    GraphContextMapping,
    GraphEdgeMapping,
    GraphNodeMapping,
    artifact_embedding_index_config,
    artifact_full_text_index_config,
    artifact_index_sources,
    graph_index_sources,
    validate_create_index_request_relationships,
)
from antfly.client_generated.models.artifact_index_source import ArtifactIndexSource
from antfly.client_generated.models.created_embeddings_index_config import (
    CreatedEmbeddingsIndexConfig,
)


def test_builds_multi_source_full_text_index() -> None:
    config = artifact_full_text_index_config("document_text", "document_text_v1", "document_chunks_v1")
    assert config == {
        "name": "document_text",
        "type": "full_text",
        "sources": [{"artifact": "document_text_v1"}, {"artifact": "document_chunks_v1"}],
    }
    assert artifact_full_text_index_config(
        "document_text",
        "document_text_v1",
        "document_chunks_v1",
        field=" text ",
        mem_only=True,
    ) == {
        "name": "document_text",
        "type": "full_text",
        "sources": [{"artifact": "document_text_v1"}, {"artifact": "document_chunks_v1"}],
        "field": "text",
        "mem_only": True,
    }
    assert artifact_full_text_index_config(
        "document_text",
        sources=[
            FullTextArtifactSource("document_text_v1", field=" summary "),
            FullTextArtifactSource("document_chunks_v1", field="text"),
        ],
    )["sources"] == [
        {"artifact": "document_text_v1", "field": "summary"},
        {"artifact": "document_chunks_v1", "field": "text"},
    ]


def test_builds_document_and_chunk_embedding_sources() -> None:
    config = artifact_embedding_index_config(
        "document_vectors",
        sources=[
            ArtifactEmbeddingSource("document_dense_v1", field="semantic_content"),
            ArtifactEmbeddingSource("document_chunk_dense_v1", source_artifact="document_chunks_v1"),
        ],
        embedder={"provider": "antfly", "model": "antflydb/clipclap"},
        dimension=384,
    )
    assert config["sources"] == [
        {"artifact": "document_dense_v1"},
        {"artifact": "document_chunk_dense_v1"},
    ]
    assert config["enrichments"][1]["source_artifact_name"] == "document_chunks_v1"
    assert all("vector_space" not in item for item in config["enrichments"])
    assert "embedding_name" not in config


def test_created_embedding_config_preserves_v0_2_single_source_fields() -> None:
    config = CreatedEmbeddingsIndexConfig(
        sources=[ArtifactIndexSource(artifact="document_dense_v1")],
        embedding_name="document_dense_v1",
        source_artifact_name="document_chunks_v1",
    )

    encoded = config.to_dict()
    assert encoded["sources"] == [{"artifact": "document_dense_v1"}]
    assert encoded["embedding_name"] == "document_dense_v1"
    assert encoded["source_artifact_name"] == "document_chunks_v1"


def test_template_only_embedding_source_omits_noop_field() -> None:
    config = artifact_embedding_index_config(
        "templated_vectors",
        sources=[ArtifactEmbeddingSource("templated_v1", template="{{ title }}: {{ body }}")],
        embedder={"provider": "antfly", "model": "antflydb/clipclap"},
    )
    assert config["enrichments"][0]["template"] == "{{ title }}: {{ body }}"
    assert "field" not in config["enrichments"][0]


def test_rejects_duplicates_and_sparse_dimensions() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        artifact_index_sources("same", "same")
    with pytest.raises(ValueError, match="dimension"):
        artifact_embedding_index_config(
            "sparse",
            sources=[ArtifactEmbeddingSource("tokens_v1")],
            embedder={"provider": "antfly", "model": "splade"},
            sparse=True,
            dimension=384,
        )


def test_graph_sources_preserve_source_specific_mapping_and_copy_metadata() -> None:
    metadata = {"origin": "extractor", "nested": {"score": 1}}
    sources = graph_index_sources(
        GraphArtifactSource(
            "relations_v1",
            path="$.relations[*]",
            nodes=GraphNodeMapping(source="{{ _doc.key }}", target=42),
            edge=GraphEdgeMapping(type="{{relation}}", metadata=metadata),
            context=GraphContextMapping(doc_fields=("title", "url")),
        ),
        GraphArtifactSource("graph_v1", path="$.graph", format="extraction_graph"),
    )
    metadata["nested"]["score"] = 2
    assert sources[0]["edge"]["metadata"]["nested"]["score"] == 1
    assert sources[0]["nodes"]["target"] == 42
    assert sources[0]["context"]["doc_fields"] == ["title", "url"]
    assert sources[1]["format"] == "extraction_graph"


def test_graph_sources_reject_duplicates_and_invalid_values() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        graph_index_sources(GraphArtifactSource("same"), GraphArtifactSource("same"))
    with pytest.raises(ValueError, match="finite"):
        graph_index_sources(GraphArtifactSource("relations", edge=GraphEdgeMapping(weight=float("nan"))))
    with pytest.raises(ValueError, match="path"):
        graph_index_sources(GraphArtifactSource("relations", path="$.relations[0]"))
    with pytest.raises(ValueError, match="nodes.source"):
        graph_index_sources(GraphArtifactSource("relations", nodes=GraphNodeMapping(source="{{ source }}")))
    for source in (
        "{{ _artifact.value.id }}{{ _doc.value.tenant_id }}",
        "{{ _artifact.value.owner-id }}",
        "{{ _artifact.value. }}",
        "{{ _artifact.value.owner.id }}",
    ):
        with pytest.raises(ValueError, match="nodes.source"):
            graph_index_sources(GraphArtifactSource("relations", nodes=GraphNodeMapping(source=source)))
    assert graph_index_sources(
        GraphArtifactSource("relations", nodes=GraphNodeMapping(source="{{ _doc.key }}"))
    )
    with pytest.raises(ValueError, match="nodes.target"):
        graph_index_sources(GraphArtifactSource("relations", nodes=GraphNodeMapping(target=float("inf"))))


def test_validates_openapi_index_request_relationships() -> None:
    with pytest.raises(ValueError, match="requires a non-empty embedding_name"):
        validate_create_index_request_relationships({"type": "embeddings", "source_artifact_name": "chunks_v1"})
    with pytest.raises(ValueError, match="external"):
        validate_create_index_request_relationships(
            {
                "type": "embeddings",
                "external": True,
                "sources": [{"artifact": "dense_v1"}],
            }
        )
    with pytest.raises(ValueError, match="authoritative embedding enrichment"):
        validate_create_index_request_relationships(
            {
                "type": "embeddings",
                "embedding_name": "dense_v1",
                "source_artifact_name": "wrong_chunks_v1",
                "enrichments": [
                    {
                        "name": "dense_v1",
                        "kind": "embedding",
                        "source_artifact_name": "chunks_v1",
                    }
                ],
            }
        )
    with pytest.raises(ValueError, match="artifact_name"):
        validate_create_index_request_relationships(
            {
                "type": "full_text",
                "artifact_name": "chunks_v1",
                "sources": [{"artifact": "chunks_v2"}],
            }
        )
    validate_create_index_request_relationships(
        {
            "type": "embeddings",
            "external": False,
            "sources": [{"artifact": "dense_v1"}],
        }
    )
