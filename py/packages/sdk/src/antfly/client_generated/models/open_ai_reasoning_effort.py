from enum import StrEnum


class OpenAIReasoningEffort(StrEnum):
    HIGH = "high"
    LOW = "low"
    MAX = "max"
    MEDIUM = "medium"
    MINIMAL = "minimal"
    NONE = "none"
    XHIGH = "xhigh"

    def __str__(self) -> str:
        return str(self.value)
