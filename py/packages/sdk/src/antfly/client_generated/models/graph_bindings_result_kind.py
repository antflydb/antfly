from enum import StrEnum


class GraphBindingsResultKind(StrEnum):
    BINDINGS = "bindings"

    def __str__(self) -> str:
        return str(self.value)
