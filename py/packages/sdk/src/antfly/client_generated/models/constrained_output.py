from enum import StrEnum


class ConstrainedOutput(StrEnum):
    AUTO = "auto"
    GRAMMAR = "grammar"
    JSON_SCHEMA = "json_schema"
    TOOL_CALL = "tool_call"

    def __str__(self) -> str:
        return str(self.value)
