from enum import StrEnum


class ReadRequirementsVisibility(StrEnum):
    ACCEPTED = "accepted"
    COMMITTED = "committed"
    PUBLISHED = "published"

    def __str__(self) -> str:
        return str(self.value)
