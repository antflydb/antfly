from enum import StrEnum


class SQLPackageVolatility(StrEnum):
    IMMUTABLE = "immutable"
    STABLE = "stable"
    VOLATILE = "volatile"

    def __str__(self) -> str:
        return str(self.value)
