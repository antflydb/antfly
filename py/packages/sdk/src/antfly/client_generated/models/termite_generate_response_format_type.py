from enum import Enum


class TermiteGenerateResponseFormatType(str, Enum):
    JSON_OBJECT = "json_object"
    JSON_SCHEMA = "json_schema"
    TEXT = "text"

    def __str__(self) -> str:
        return str(self.value)
