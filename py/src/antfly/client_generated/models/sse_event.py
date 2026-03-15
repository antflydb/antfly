from enum import Enum


class SSEEvent(str, Enum):
    CLASSIFICATION = "classification"
    DONE = "done"
    ERROR = "error"
    EVAL = "eval"
    FOLLOWUP = "followup"
    GENERATION = "generation"
    HIT = "hit"
    REASONING = "reasoning"
    STEP_COMPLETED = "step_completed"
    STEP_PROGRESS = "step_progress"
    STEP_STARTED = "step_started"
    TOOL_MODE = "tool_mode"

    def __str__(self) -> str:
        return str(self.value)
