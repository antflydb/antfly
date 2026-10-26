from enum import StrEnum


class DocumentSubfieldMappingMissingNullPolicy(StrEnum):
    MISSING_REJECTED = "missing_rejected"
    NULLS_ORDERED = "nulls_ordered"

    def __str__(self) -> str:
        return str(self.value)
