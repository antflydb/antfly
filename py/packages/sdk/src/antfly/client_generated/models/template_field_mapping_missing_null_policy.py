from enum import StrEnum


class TemplateFieldMappingMissingNullPolicy(StrEnum):
    MISSING_REJECTED = "missing_rejected"

    def __str__(self) -> str:
        return str(self.value)
