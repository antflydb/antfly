from enum import Enum


class TermiteGenerateChunkObject(str, Enum):
    CHAT_COMPLETION_CHUNK = "chat.completion.chunk"

    def __str__(self) -> str:
        return str(self.value)
