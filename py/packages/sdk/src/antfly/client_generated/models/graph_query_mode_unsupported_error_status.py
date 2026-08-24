from enum import IntEnum


class GraphQueryModeUnsupportedErrorStatus(IntEnum):
    VALUE_422 = 422

    def __str__(self) -> str:
        return str(self.value)
