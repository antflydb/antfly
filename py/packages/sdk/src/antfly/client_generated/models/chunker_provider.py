from enum import StrEnum


class ChunkerProvider(StrEnum):
    ANTFLY = "antfly"
    MOCK = "mock"

    def __str__(self) -> str:
        return str(self.value)
