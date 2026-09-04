from enum import StrEnum


class ExternalIoProtocol(StrEnum):
    FILESYSTEM = "filesystem"
    GCS = "gcs"
    HTTP = "http"
    S3 = "s3"

    def __str__(self) -> str:
        return str(self.value)
