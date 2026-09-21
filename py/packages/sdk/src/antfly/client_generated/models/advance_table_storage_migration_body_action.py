from enum import StrEnum


class AdvanceTableStorageMigrationBodyAction(StrEnum):
    CANCEL = "cancel"
    PUBLISH = "publish"
    STEP = "step"

    def __str__(self) -> str:
        return str(self.value)
