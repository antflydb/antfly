from enum import Enum


class RowsRowClaimMode(str, Enum):
    FOR_NO_KEY_UPDATE = "for_no_key_update"
    FOR_UPDATE = "for_update"

    def __str__(self) -> str:
        return str(self.value)
