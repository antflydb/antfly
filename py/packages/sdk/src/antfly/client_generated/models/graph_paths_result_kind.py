from enum import Enum


class GraphPathsResultKind(str, Enum):
    PATHS = "paths"

    def __str__(self) -> str:
        return str(self.value)
