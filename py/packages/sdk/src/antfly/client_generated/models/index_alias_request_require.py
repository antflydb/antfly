from enum import StrEnum


class IndexAliasRequestRequire(StrEnum):
    COMPLETE = "complete"
    QUERYABLE = "queryable"

    def __str__(self) -> str:
        return str(self.value)
