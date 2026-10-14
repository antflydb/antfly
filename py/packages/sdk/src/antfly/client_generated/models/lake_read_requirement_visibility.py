from enum import StrEnum


class LakeReadRequirementVisibility(StrEnum):
    ACCEPTED = "accepted"
    PUBLISHED = "published"

    def __str__(self) -> str:
        return str(self.value)
