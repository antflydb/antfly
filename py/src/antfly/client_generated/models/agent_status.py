from enum import Enum


class AgentStatus(str, Enum):
    CLARIFICATION_REQUIRED = "clarification_required"
    COMPLETED = "completed"
    FAILED = "failed"
    INCOMPLETE = "incomplete"
    IN_PROGRESS = "in_progress"

    def __str__(self) -> str:
        return str(self.value)
