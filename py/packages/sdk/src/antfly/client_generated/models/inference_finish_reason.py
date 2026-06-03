from enum import Enum


class InferenceFinishReason(str, Enum):
    CONTENT_FILTER = "content_filter"
    FUNCTION_CALL = "function_call"
    LENGTH = "length"
    STOP = "stop"
    TOOL_CALLS = "tool_calls"

    def __str__(self) -> str:
        return str(self.value)
