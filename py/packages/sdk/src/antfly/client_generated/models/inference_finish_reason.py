from enum import StrEnum


class InferenceFinishReason(StrEnum):
    CONTENT_FILTER = "content_filter"
    FUNCTION_CALL = "function_call"
    LENGTH = "length"
    STOP = "stop"
    TOOL_CALLS = "tool_calls"

    def __str__(self) -> str:
        return str(self.value)
