from enum import StrEnum


class DynamicTemplateMatchMappingType(StrEnum):
    BOOLEAN = "boolean"
    DATE = "date"
    NUMBER = "number"
    OBJECT = "object"
    STRING = "string"

    def __str__(self) -> str:
        return str(self.value)
