from enum import StrEnum


class TrainedChoiceAnswerType(StrEnum):
    CHOICE = "choice"

    def __str__(self) -> str:
        return str(self.value)
