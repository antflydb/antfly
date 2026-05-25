from enum import Enum


class IncompleteDetailsReason(str, Enum):
    CLARIFICATION_REQUIRED = "clarification_required"
    MAX_INTERNAL_ITERATIONS = "max_internal_iterations"
    MAX_TOKENS = "max_tokens"
    NO_TOOLS = "no_tools"

    def __str__(self) -> str:
        return str(self.value)
