from enum import StrEnum


class ArtifactView(StrEnum):
    DESCRIPTIVE = "descriptive"
    SEED = "seed"

    def __str__(self) -> str:
        return str(self.value)
