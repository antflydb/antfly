from enum import StrEnum


class SqlSettingPutKind(StrEnum):
    BOOLEAN = "boolean"
    INTEGER = "integer"
    STRING = "string"

    def __str__(self) -> str:
        return str(self.value)
