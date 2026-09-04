from enum import StrEnum


class InferenceGenerateChunkObject(StrEnum):
    CHAT_COMPLETION_CHUNK = "chat.completion.chunk"

    def __str__(self) -> str:
        return str(self.value)
