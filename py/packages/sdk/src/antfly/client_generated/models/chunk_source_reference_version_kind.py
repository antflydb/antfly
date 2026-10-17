from enum import StrEnum


class ChunkSourceReferenceVersionKind(StrEnum):
    S3_VERSION_ID = "s3_version_id"
    STRONG_ETAG = "strong_etag"

    def __str__(self) -> str:
        return str(self.value)
