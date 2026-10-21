from enum import StrEnum


class ChunkEmbeddingTerminalEventObject(StrEnum):
    CHUNK_EMBEDDING_COMPLETE = "chunk.embedding.complete"

    def __str__(self) -> str:
        return str(self.value)
