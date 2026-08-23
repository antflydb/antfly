from enum import IntEnum


class GraphCrossRangeModeUnsupportedErrorStatus(IntEnum):
    VALUE_422 = 422

    def __str__(self) -> str:
        return str(self.value)
