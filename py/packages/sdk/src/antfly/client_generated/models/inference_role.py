from enum import StrEnum


class InferenceRole(StrEnum):
    ASSISTANT = "assistant"
    DEVELOPER = "developer"
    SYSTEM = "system"
    TOOL = "tool"
    USER = "user"

    def __str__(self) -> str:
        return str(self.value)
