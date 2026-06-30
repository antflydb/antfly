from enum import Enum


class RowsArrayUpdateTransformOp(str, Enum):
    ADD_TO_SET = "add_to_set"
    APPEND = "append"
    REMOVE = "remove"

    def __str__(self) -> str:
        return str(self.value)
