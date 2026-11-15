from enum import StrEnum


class IndexAliasRequire(StrEnum):
    COMPLETE = "complete"
    QUERYABLE = "queryable"

    def __str__(self) -> str:
        return str(self.value)
