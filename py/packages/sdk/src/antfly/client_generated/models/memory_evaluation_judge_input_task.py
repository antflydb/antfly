from enum import StrEnum


class MemoryEvaluationJudgeInputTask(StrEnum):
    IDENTITY = "identity"
    RELATIONSHIP = "relationship"
    RETRIEVAL = "retrieval"
    SUPERSESSION = "supersession"

    def __str__(self) -> str:
        return str(self.value)
