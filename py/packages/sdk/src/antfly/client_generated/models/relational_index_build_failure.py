from enum import StrEnum


class RelationalIndexBuildFailure(StrEnum):
    INCOMPATIBLE_SCHEMA = "incompatible_schema"
    INVALID_ROW = "invalid_row"
    KEY_TOO_LARGE = "key_too_large"

    def __str__(self) -> str:
        return str(self.value)
