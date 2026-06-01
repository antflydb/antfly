from enum import Enum


class ChunkerProvider(str, Enum):
    ANTFLY = "antfly"
    MOCK = "mock"

    def __str__(self) -> str:
        return str(self.value)
