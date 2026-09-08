from enum import StrEnum


class GraphPathsResultKind(StrEnum):
    PATHS = "paths"

    def __str__(self) -> str:
        return str(self.value)
