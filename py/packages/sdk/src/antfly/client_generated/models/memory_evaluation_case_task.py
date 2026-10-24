from enum import StrEnum


class MemoryEvaluationCaseTask(StrEnum):
    IDENTITY = "identity"
    RELATIONSHIP = "relationship"
    RETRIEVAL = "retrieval"
    SUPERSESSION = "supersession"

    def __str__(self) -> str:
        return str(self.value)
