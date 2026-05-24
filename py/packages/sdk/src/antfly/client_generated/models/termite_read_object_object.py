from enum import Enum


class TermiteReadObjectObject(str, Enum):
    READ = "read"

    def __str__(self) -> str:
        return str(self.value)
