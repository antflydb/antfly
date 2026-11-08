from enum import StrEnum


class MemoryEvaluationPolicyInputRetrieval(StrEnum):
    BRIEF = "brief"
    NONE = "none"
    RECALL = "recall"

    def __str__(self) -> str:
        return str(self.value)
