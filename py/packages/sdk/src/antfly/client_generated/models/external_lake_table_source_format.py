from enum import StrEnum


class ExternalLakeTableSourceFormat(StrEnum):
    ICEBERG = "iceberg"
    PARQUET = "parquet"

    def __str__(self) -> str:
        return str(self.value)
