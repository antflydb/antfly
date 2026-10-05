from enum import StrEnum


class ExternalLakeTableSourceWritePolicy(StrEnum):
    READ_ONLY = "read_only"

    def __str__(self) -> str:
        return str(self.value)
