from enum import StrEnum


class ForeignKeyAction(StrEnum):
    CASCADE = "cascade"
    NO_ACTION = "no_action"
    RESTRICT = "restrict"
    SET_NULL = "set_null"

    def __str__(self) -> str:
        return str(self.value)
