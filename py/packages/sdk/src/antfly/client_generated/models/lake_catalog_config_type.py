from enum import StrEnum


class LakeCatalogConfigType(StrEnum):
    MANAGED = "managed"
    REST = "rest"

    def __str__(self) -> str:
        return str(self.value)
