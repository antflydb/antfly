from enum import StrEnum


class ResearchSubQuestionStatus(StrEnum):
    FAILED = "failed"
    PENDING = "pending"
    RESEARCHED = "researched"
    SKIPPED = "skipped"

    def __str__(self) -> str:
        return str(self.value)
