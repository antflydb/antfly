from enum import StrEnum


class EntityNamespaceOutcomeAbsorbedItemDisposition(StrEnum):
    RELEASE = "release"
    SUPPRESS = "suppress"

    def __str__(self) -> str:
        return str(self.value)
