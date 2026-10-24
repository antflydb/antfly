from enum import StrEnum


class MemoryEvaluationJudgeOutputLabel(StrEnum):
    CONTRADICTS = "contradicts"
    DIFFERENT = "different"
    INDEPENDENT = "independent"
    RELATED = "related"
    RESTATES = "restates"
    SAME = "same"
    SUPERSEDES = "supersedes"
    UNRELATED = "unrelated"

    def __str__(self) -> str:
        return str(self.value)
