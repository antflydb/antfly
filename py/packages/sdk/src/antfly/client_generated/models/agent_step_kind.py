from enum import StrEnum


class AgentStepKind(StrEnum):
    CLARIFICATION = "clarification"
    CLASSIFICATION = "classification"
    GENERATION = "generation"
    PLANNING = "planning"
    TOOL_CALL = "tool_call"
    VALIDATION = "validation"

    def __str__(self) -> str:
        return str(self.value)
