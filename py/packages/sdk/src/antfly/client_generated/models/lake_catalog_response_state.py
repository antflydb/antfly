from enum import StrEnum


class LakeCatalogResponseState(StrEnum):
    COMMITTED = "committed"
    LAKE_COMMITTED = "lake_committed"
    LOADED = "loaded"
    NOT_COMMITTED = "not_committed"
    UNKNOWN = "unknown"

    def __str__(self) -> str:
        return str(self.value)
