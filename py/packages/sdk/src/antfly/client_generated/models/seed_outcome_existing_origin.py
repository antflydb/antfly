from enum import StrEnum


class SeedOutcomeExistingOrigin(StrEnum):
    EXTERNAL = "external"
    GENERATED = "generated"
    SEEDED = "seeded"

    def __str__(self) -> str:
        return str(self.value)
