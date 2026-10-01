from enum import StrEnum


class STTProvider(StrEnum):
    ANTFLY = "antfly"
    OPENAI = "openai"
    VERTEX = "vertex"

    def __str__(self) -> str:
        return str(self.value)
