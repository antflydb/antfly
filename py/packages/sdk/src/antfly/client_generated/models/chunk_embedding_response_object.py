from enum import StrEnum


class ChunkEmbeddingResponseObject(StrEnum):
    CHUNK_EMBEDDING_LIST = "chunk.embedding.list"

    def __str__(self) -> str:
        return str(self.value)
