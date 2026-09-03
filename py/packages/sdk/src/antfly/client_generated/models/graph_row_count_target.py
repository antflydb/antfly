from enum import Enum


class GraphRowCountTarget(str, Enum):
    VALUE_0 = "*"

    def __str__(self) -> str:
        return str(self.value)
