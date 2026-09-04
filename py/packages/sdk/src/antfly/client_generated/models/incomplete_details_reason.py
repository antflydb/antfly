from enum import StrEnum


class IncompleteDetailsReason(StrEnum):
    CLARIFICATION_REQUIRED = "clarification_required"
    MAX_INTERNAL_ITERATIONS = "max_internal_iterations"
    MAX_TOKENS = "max_tokens"
    NO_TOOLS = "no_tools"

    def __str__(self) -> str:
        return str(self.value)
