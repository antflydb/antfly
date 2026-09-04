from enum import StrEnum


class AgentStepStatus(StrEnum):
    ERROR = "error"
    SKIPPED = "skipped"
    SUCCESS = "success"

    def __str__(self) -> str:
        return str(self.value)
