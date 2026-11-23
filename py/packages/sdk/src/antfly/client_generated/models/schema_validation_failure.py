from enum import StrEnum


class SchemaValidationFailure(StrEnum):
    FOREIGN_KEY_VIOLATION = "foreign_key_violation"
    NOT_NULL_VIOLATION = "not_null_violation"
    OWNER_FAILED = "owner_failed"
    REQUESTED = "requested"

    def __str__(self) -> str:
        return str(self.value)
