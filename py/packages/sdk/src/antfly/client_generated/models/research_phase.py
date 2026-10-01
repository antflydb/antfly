from enum import StrEnum


class ResearchPhase(StrEnum):
    DONE = "done"
    PLAN = "plan"
    REFLECT = "reflect"
    RESEARCH = "research"
    VERIFY = "verify"
    WRITE = "write"

    def __str__(self) -> str:
        return str(self.value)
