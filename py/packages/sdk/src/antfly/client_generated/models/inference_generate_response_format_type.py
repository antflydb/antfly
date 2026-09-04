from enum import StrEnum


class InferenceGenerateResponseFormatType(StrEnum):
    JSON_OBJECT = "json_object"
    JSON_SCHEMA = "json_schema"
    TEXT = "text"

    def __str__(self) -> str:
        return str(self.value)
