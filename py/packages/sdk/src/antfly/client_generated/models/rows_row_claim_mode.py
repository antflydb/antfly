from enum import Enum


class RowsRowClaimMode(str, Enum):
    FOR_UPDATE = "for_update"

    def __str__(self) -> str:
        return str(self.value)
