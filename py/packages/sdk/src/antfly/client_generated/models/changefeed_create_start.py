from enum import StrEnum


class ChangefeedCreateStart(StrEnum):
    NOW = "now"
    SNAPSHOT_THEN_CHANGES = "snapshot_then_changes"

    def __str__(self) -> str:
        return str(self.value)
