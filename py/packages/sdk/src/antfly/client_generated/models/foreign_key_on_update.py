from enum import Enum


class ForeignKeyOnUpdate(str, Enum):
    CASCADE = "cascade"
    NO_ACTION = "no_action"
    RESTRICT = "restrict"
    SET_NULL = "set_null"

    def __str__(self) -> str:
        return str(self.value)
