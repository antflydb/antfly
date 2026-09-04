from enum import StrEnum


class ClusterBackupRequestFormat(StrEnum):
    NATIVE = "native"
    PORTABLE = "portable"

    def __str__(self) -> str:
        return str(self.value)
