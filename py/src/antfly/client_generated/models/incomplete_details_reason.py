from enum import Enum


class IncompleteDetailsReason(str, Enum):
    CLARIFICATION_NEEDED = "clarification_needed"
    MAX_ITERATIONS = "max_iterations"
    MAX_TOKENS = "max_tokens"
    NO_TOOLS = "no_tools"

    def __str__(self) -> str:
        return str(self.value)
