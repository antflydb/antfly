from enum import StrEnum


class ChangefeedEventOperation(StrEnum):
    DELETE = "delete"
    ENTER = "enter"
    INSERT = "insert"
    LEAVE = "leave"
    UPDATE = "update"

    def __str__(self) -> str:
        return str(self.value)
