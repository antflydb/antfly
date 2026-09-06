from enum import StrEnum


class InferenceWarmModelStartupStrategy(StrEnum):
    EAGER = "eager"
    PREFETCH = "prefetch"

    def __str__(self) -> str:
        return str(self.value)
