from enum import StrEnum


class IndexPublicationState(StrEnum):
    ADMISSION_UNKNOWN = "admission_unknown"
    CANCELED = "canceled"
    FAILED = "failed"
    PENDING = "pending"
    PUBLISHED = "published"

    def __str__(self) -> str:
        return str(self.value)
