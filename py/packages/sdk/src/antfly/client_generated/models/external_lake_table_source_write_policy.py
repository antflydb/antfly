from enum import StrEnum


class ExternalLakeTableSourceWritePolicy(StrEnum):
    ICEBERG_WRITER = "iceberg_writer"
    READ_ONLY = "read_only"

    def __str__(self) -> str:
        return str(self.value)
