from enum import StrEnum


class AgentStatus(StrEnum):
    CLARIFICATION_REQUIRED = "clarification_required"
    COMPLETED = "completed"
    FAILED = "failed"
    INCOMPLETE = "incomplete"
    IN_PROGRESS = "in_progress"

    def __str__(self) -> str:
        return str(self.value)
