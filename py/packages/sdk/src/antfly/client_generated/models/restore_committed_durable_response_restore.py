from enum import Enum


class RestoreCommittedDurableResponseRestore(str, Enum):
    COMMITTED = "committed"

    def __str__(self) -> str:
        return str(self.value)
