import pytest

from antfly import (
    ArtifactEmbeddingSource,
    artifact_embedding_index_config,
    artifact_index_sources,
    graph_index_sources,
)


def test_multi_source_embedding_config_uses_automatic_vector_space() -> None:
    config = artifact_embedding_index_config(
        "document_vectors",
        sources=[
            ArtifactEmbeddingSource("title_dense_v1", "title_chunks_v1"),
            ArtifactEmbeddingSource("body_dense_v1", "body_chunks_v1"),
        ],
        embedder={"provider": "antfly", "model": "antflydb/clipclap"},
        dimension=384,
    )
    assert config["sources"] == [
        {"artifact": "title_dense_v1"},
        {"artifact": "body_dense_v1"},
    ]
    assert all("vector_space" not in enrichment for enrichment in config["enrichments"])
    assert "field" not in config
    assert "embedding_name" not in config


def test_graph_sources_preserve_path_and_format() -> None:
    sources = graph_index_sources(
        {"artifact": "relations_v1", "path": "$.relations[*]", "format": "extraction_relation"},
        {"artifact": "graph_v1", "path": "$.graph", "format": "extraction_graph"},
    )
    assert sources[1]["format"] == "extraction_graph"


def test_artifact_source_validation() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        artifact_index_sources("same", "same")
    with pytest.raises(ValueError, match="at most 64"):
        artifact_index_sources(*(f"a{i}" for i in range(65)))
    with pytest.raises(ValueError, match="format"):
        graph_index_sources({"artifact": "relations_v1", "format": "unknown"})
    with pytest.raises(ValueError, match="provider"):
        artifact_embedding_index_config(
            "vectors",
            sources=[ArtifactEmbeddingSource("dense_v1")],
            embedder={},
        )
