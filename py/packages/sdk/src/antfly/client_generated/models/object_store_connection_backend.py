from enum import Enum


class ObjectStoreConnectionBackend(str, Enum):
    FILESYSTEM = "filesystem"
    GCS = "gcs"
    S3 = "s3"

    def __str__(self) -> str:
        return str(self.value)
