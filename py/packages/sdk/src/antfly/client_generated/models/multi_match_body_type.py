from enum import StrEnum


class MultiMatchBodyType(StrEnum):
    BOOL_PREFIX = "bool_prefix"

    def __str__(self) -> str:
        return str(self.value)
