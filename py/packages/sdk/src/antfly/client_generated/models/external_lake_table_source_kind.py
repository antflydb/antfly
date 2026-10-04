from enum import StrEnum


class ExternalLakeTableSourceKind(StrEnum):
    EXTERNAL = "external"

    def __str__(self) -> str:
        return str(self.value)
