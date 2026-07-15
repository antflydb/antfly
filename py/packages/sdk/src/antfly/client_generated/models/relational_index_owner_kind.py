from enum import Enum


class RelationalIndexOwnerKind(str, Enum):
    RELATIONAL_COLUMN = "relational_column"
    TABLE = "table"
    UNIQUE_CONSTRAINT = "unique_constraint"

    def __str__(self) -> str:
        return str(self.value)
