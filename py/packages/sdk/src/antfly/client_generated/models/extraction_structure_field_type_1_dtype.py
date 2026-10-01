from enum import StrEnum


class ExtractionStructureFieldType1Dtype(StrEnum):
    LIST = "list"
    STR = "str"

    def __str__(self) -> str:
        return str(self.value)
