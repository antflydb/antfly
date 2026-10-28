from enum import StrEnum


class SQLNamedEnumColumnSchemaType(StrEnum):
    SQL_ENUM = "sql_enum"

    def __str__(self) -> str:
        return str(self.value)
