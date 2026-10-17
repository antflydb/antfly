from enum import StrEnum


class ChunkEmbeddingStartEventObject(StrEnum):
    CHUNK_EMBEDDING_START = "chunk.embedding.start"

    def __str__(self) -> str:
        return str(self.value)
