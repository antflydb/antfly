from enum import StrEnum


class ExtractionEntityDefinitionType(StrEnum):
    ARRAY = "array"
    LIST = "list"
    STR = "str"
    STRING = "string"

    def __str__(self) -> str:
        return str(self.value)
