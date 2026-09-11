from enum import StrEnum


class LegacyGraphSearchResultKind(StrEnum):
    LEGACY = "legacy"

    def __str__(self) -> str:
        return str(self.value)
