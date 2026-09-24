from enum import StrEnum


class InferenceDictateResponseObject(StrEnum):
    DICTATION = "dictation"

    def __str__(self) -> str:
        return str(self.value)
