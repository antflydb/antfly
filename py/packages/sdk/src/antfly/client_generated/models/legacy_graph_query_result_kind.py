from enum import Enum


class LegacyGraphQueryResultKind(str, Enum):
    LEGACY = "legacy"

    def __str__(self) -> str:
        return str(self.value)
