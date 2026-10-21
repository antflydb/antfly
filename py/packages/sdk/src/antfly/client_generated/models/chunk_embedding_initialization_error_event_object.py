from enum import StrEnum


class ChunkEmbeddingInitializationErrorEventObject(StrEnum):
    CHUNK_EMBEDDING_ERROR = "chunk.embedding.error"

    def __str__(self) -> str:
        return str(self.value)
