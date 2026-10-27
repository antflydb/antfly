from enum import StrEnum


class InferenceDocumentJobOperation(StrEnum):
    EXTRACT = "extract"
    READ = "read"

    def __str__(self) -> str:
        return str(self.value)
