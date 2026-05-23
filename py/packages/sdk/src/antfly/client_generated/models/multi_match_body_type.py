from enum import Enum


class MultiMatchBodyType(str, Enum):
    BOOL_PREFIX = "bool_prefix"

    def __str__(self) -> str:
        return str(self.value)
