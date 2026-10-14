from enum import StrEnum


class SQLRequestLakeVisibility(StrEnum):
    ACCEPTED = "accepted"
    COMMITTED = "committed"

    def __str__(self) -> str:
        return str(self.value)
