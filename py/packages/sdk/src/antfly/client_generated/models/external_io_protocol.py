from enum import Enum


class ExternalIoProtocol(str, Enum):
    FILESYSTEM = "filesystem"
    GCS = "gcs"
    HTTP = "http"
    S3 = "s3"

    def __str__(self) -> str:
        return str(self.value)
