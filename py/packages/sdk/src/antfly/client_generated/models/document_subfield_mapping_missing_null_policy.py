from enum import StrEnum


class DocumentSubfieldMappingMissingNullPolicy(StrEnum):
    MISSING_REJECTED = "missing_rejected"

    def __str__(self) -> str:
        return str(self.value)
