from enum import Enum


class ClusterRestoreRequestRestoreMode(str, Enum):
    FAIL_IF_EXISTS = "fail_if_exists"
    OVERWRITE = "overwrite"
    SKIP_IF_EXISTS = "skip_if_exists"

    def __str__(self) -> str:
        return str(self.value)
