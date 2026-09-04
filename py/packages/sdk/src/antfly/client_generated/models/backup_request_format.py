from enum import StrEnum


class BackupRequestFormat(StrEnum):
    NATIVE = "native"
    PORTABLE = "portable"

    def __str__(self) -> str:
        return str(self.value)
