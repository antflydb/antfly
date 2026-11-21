from enum import StrEnum


class ArtifactNotCurrentError(StrEnum):
    ARTIFACT_NOT_CURRENT = "artifact_not_current"

    def __str__(self) -> str:
        return str(self.value)
