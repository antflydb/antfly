from enum import StrEnum


class SeedOutcomeVia(StrEnum):
    EXPLICIT = "explicit"
    MAPPING = "mapping"

    def __str__(self) -> str:
        return str(self.value)
