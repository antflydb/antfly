from enum import StrEnum


class ResearchIncompleteDetailsReason(StrEnum):
    CANCELLED = "cancelled"
    CLARIFICATION_REQUIRED = "clarification_required"
    DEADLINE = "deadline"
    MAX_LLM_CALLS = "max_llm_calls"
    MAX_ROUNDS = "max_rounds"
    MAX_TOOL_CALLS = "max_tool_calls"
    NO_EVIDENCE = "no_evidence"
    PHASE_LIMIT = "phase_limit"

    def __str__(self) -> str:
        return str(self.value)
