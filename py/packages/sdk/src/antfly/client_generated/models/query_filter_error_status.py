from enum import IntEnum


class QueryFilterErrorStatus(IntEnum):
    VALUE_400 = 400
    VALUE_422 = 422

    def __str__(self) -> str:
        return str(self.value)
