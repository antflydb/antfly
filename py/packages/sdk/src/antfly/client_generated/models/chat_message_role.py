from enum import StrEnum


class ChatMessageRole(StrEnum):
    ASSISTANT = "assistant"
    SYSTEM = "system"
    TOOL = "tool"
    USER = "user"

    def __str__(self) -> str:
        return str(self.value)
