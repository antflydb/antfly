from enum import Enum


class SqlStatementResponseKind(str, Enum):
    DDL = "ddl"
    READ = "read"
    WRITE = "write"

    def __str__(self) -> str:
        return str(self.value)
