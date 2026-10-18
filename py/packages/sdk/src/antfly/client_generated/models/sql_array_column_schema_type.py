from enum import StrEnum


class SQLArrayColumnSchemaType(StrEnum):
    SQL_ARRAY = "sql_array"

    def __str__(self) -> str:
        return str(self.value)
