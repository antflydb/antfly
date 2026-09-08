from enum import StrEnum


class InferenceGenerateResponseObject(StrEnum):
    CHAT_COMPLETION = "chat.completion"

    def __str__(self) -> str:
        return str(self.value)
