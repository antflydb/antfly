from enum import StrEnum


class ExtractionStructureFieldType1Type(StrEnum):
    ARRAY = "array"
    LIST = "list"
    STR = "str"
    STRING = "string"

    def __str__(self) -> str:
        return str(self.value)
