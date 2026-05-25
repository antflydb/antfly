from enum import Enum


class ClusterBackupRequestFormat(str, Enum):
    NATIVE = "native"
    PORTABLE = "portable"

    def __str__(self) -> str:
        return str(self.value)
