from enum import StrEnum


class ExtractionStructureFieldType1Type(StrEnum):
    ARRAY = "array"
    DATE = "date"
    ESTIMATE = "estimate"
    LIST = "list"
    NUMBER = "number"
    STR = "str"
    STRING = "string"
    TEXT = "text"

    def __str__(self) -> str:
        return str(self.value)
