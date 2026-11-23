from enum import StrEnum


class SchemaValidationState(StrEnum):
    ADMISSION_UNKNOWN = "admission_unknown"
    CANCELED = "canceled"
    FAILED = "failed"
    PENDING = "pending"
    SUCCEEDED = "succeeded"

    def __str__(self) -> str:
        return str(self.value)
