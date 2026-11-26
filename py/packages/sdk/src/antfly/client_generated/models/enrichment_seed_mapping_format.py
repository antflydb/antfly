from enum import StrEnum


class EnrichmentSeedMappingFormat(StrEnum):
    AUTO = "auto"
    CHUNKS = "chunks"
    CHUNK_VECTORS = "chunk_vectors"
    JSON = "json"
    TEXT = "text"
    VECTOR = "vector"

    def __str__(self) -> str:
        return str(self.value)
