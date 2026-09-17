from enum import StrEnum


class RetrievalNavigationSelection(StrEnum):
    AGENTIC = "agentic"
    RANKED = "ranked"

    def __str__(self) -> str:
        return str(self.value)
