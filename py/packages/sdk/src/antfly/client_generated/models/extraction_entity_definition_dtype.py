from enum import StrEnum


class ExtractionEntityDefinitionDtype(StrEnum):
    LIST = "list"
    STR = "str"

    def __str__(self) -> str:
        return str(self.value)
