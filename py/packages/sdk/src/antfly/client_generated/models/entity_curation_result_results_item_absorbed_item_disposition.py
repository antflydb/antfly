from enum import StrEnum


class EntityCurationResultResultsItemAbsorbedItemDisposition(StrEnum):
    RELEASE = "release"
    SUPPRESS = "suppress"

    def __str__(self) -> str:
        return str(self.value)
