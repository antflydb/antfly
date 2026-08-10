from enum import Enum


class ExtractionStructureFieldType1Type(str, Enum):
    ARRAY = "array"
    LIST = "list"
    STR = "str"
    STRING = "string"

    def __str__(self) -> str:
        return str(self.value)
