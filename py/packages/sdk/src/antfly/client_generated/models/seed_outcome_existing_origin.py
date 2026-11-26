from enum import StrEnum


class SeedOutcomeExistingOrigin(StrEnum):
    GENERATED = "generated"
    SEEDED = "seeded"

    def __str__(self) -> str:
        return str(self.value)
