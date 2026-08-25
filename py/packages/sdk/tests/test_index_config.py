import pytest

from antfly import (
    ArtifactEmbeddingSource,
    GraphArtifactSource,
    GraphContextMapping,
    GraphEdgeMapping,
    GraphNodeMapping,
    artifact_embedding_index_config,
    artifact_full_text_index_config,
    artifact_index_sources,
    graph_index_sources,
)


def test_builds_multi_source_full_text_index() -> None:
    config = artifact_full_text_index_config("document_text", "document_text_v1", "document_chunks_v1")
    assert config == {
        "name": "document_text",
        "type": "full_text",
        "sources": [{"artifact": "document_text_v1"}, {"artifact": "document_chunks_v1"}],
    }


def test_builds_document_and_chunk_embedding_sources() -> None:
    config = artifact_embedding_index_config(
        "document_vectors",
        sources=[
            ArtifactEmbeddingSource("document_dense_v1", field="semantic_content"),
            ArtifactEmbeddingSource("document_chunk_dense_v1", source_artifact="document_chunks_v1"),
        ],
        embedder={"provider": "antfly", "model": "antflydb/clipclap"},
        dimension=384,
        vector_space="searchaf:v1",
    )
    assert config["sources"] == [
        {"artifact": "document_dense_v1"},
        {"artifact": "document_chunk_dense_v1"},
    ]
    assert config["enrichments"][1]["source_artifact_name"] == "document_chunks_v1"
    assert all(item["vector_space"] == "searchaf:v1" for item in config["enrichments"])
    assert "embedding_name" not in config


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
            nodes=GraphNodeMapping(source="{{source}}", target="{{target}}"),
            edge=GraphEdgeMapping(type="{{relation}}", metadata=metadata),
            context=GraphContextMapping(doc_fields=("title", "url")),
        ),
        GraphArtifactSource("graph_v1", path="$.graph", format="extraction_graph"),
    )
    metadata["nested"]["score"] = 2
    assert sources[0]["edge"]["metadata"]["nested"]["score"] == 1
    assert sources[0]["context"]["doc_fields"] == ["title", "url"]
    assert sources[1]["format"] == "extraction_graph"


def test_graph_sources_reject_duplicates_and_invalid_values() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        graph_index_sources(GraphArtifactSource("same"), GraphArtifactSource("same"))
    with pytest.raises(ValueError, match="finite"):
        graph_index_sources(GraphArtifactSource("relations", edge=GraphEdgeMapping(weight=float("nan"))))
