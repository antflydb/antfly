from enum import Enum


class GraphBindingsResultKind(str, Enum):
    BINDINGS = "bindings"

    def __str__(self) -> str:
        return str(self.value)
