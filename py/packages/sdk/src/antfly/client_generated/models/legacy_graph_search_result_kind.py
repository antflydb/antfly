from enum import Enum


class LegacyGraphSearchResultKind(str, Enum):
    LEGACY = "legacy"

    def __str__(self) -> str:
        return str(self.value)
