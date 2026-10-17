from enum import StrEnum


class ChunkEmbeddingEventObject(StrEnum):
    CHUNK_EMBEDDING = "chunk.embedding"

    def __str__(self) -> str:
        return str(self.value)
