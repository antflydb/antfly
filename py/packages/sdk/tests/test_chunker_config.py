from antfly.client_generated.models.chunker_config import ChunkerConfig
from antfly.client_generated.models.chunker_provider import ChunkerProvider
from antfly.client_generated.models.created_embeddings_index import CreatedEmbeddingsIndex


def test_chunker_config_exposes_effective_provider_fields() -> None:
    created = CreatedEmbeddingsIndex.from_dict(
        {
            "name": "semantic",
            "type": "embeddings",
            "dimension": 3,
            "chunker": {
                "provider": "antfly",
                "api_url": "http://inference.internal:8080",
                "model": "fixed",
                "store_chunks": False,
            },
        }
    )
    assert isinstance(created.chunker, ChunkerConfig)
    chunker = created.chunker

    assert chunker.provider is ChunkerProvider.ANTFLY
    assert chunker.model == "fixed"
    assert chunker.api_url == "http://inference.internal:8080"
    assert chunker.to_dict()["model"] == "fixed"
    assert chunker.to_dict()["api_url"] == "http://inference.internal:8080"
