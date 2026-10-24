from enum import StrEnum


class MemoryExplanationJustification(StrEnum):
    RECORDED = "recorded"

    def __str__(self) -> str:
        return str(self.value)
