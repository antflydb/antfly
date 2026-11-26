from enum import StrEnum


class SeedConflictPolicy(StrEnum):
    KEEP = "keep"
    REPLACE = "replace"

    def __str__(self) -> str:
        return str(self.value)
