from enum import StrEnum


class ChatGPTGeneratorConfigProvider(StrEnum):
    CHATGPT = "chatgpt"

    def __str__(self) -> str:
        return str(self.value)
