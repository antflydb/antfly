from enum import Enum


class TemplateFieldMappingMissingNullPolicy(str, Enum):
    MISSING_REJECTED = "missing_rejected"

    def __str__(self) -> str:
        return str(self.value)
