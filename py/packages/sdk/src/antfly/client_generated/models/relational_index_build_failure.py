from enum import StrEnum


class RelationalIndexBuildFailure(StrEnum):
    INCOMPATIBLE_SCHEMA = "incompatible_schema"
    INVALID_ROW = "invalid_row"

    def __str__(self) -> str:
        return str(self.value)
