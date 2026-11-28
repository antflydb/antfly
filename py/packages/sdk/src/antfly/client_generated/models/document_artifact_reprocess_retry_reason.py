from enum import StrEnum


class DocumentArtifactReprocessRetryReason(StrEnum):
    PROVIDER_CAPACITY = "provider_capacity"
    PROVIDER_RETRY_AFTER = "provider_retry_after"
    TRANSIENT = "transient"

    def __str__(self) -> str:
        return str(self.value)
