from enum import Enum


class AgentStepStatus(str, Enum):
    ERROR = "error"
    SKIPPED = "skipped"
    SUCCESS = "success"

    def __str__(self) -> str:
        return str(self.value)
