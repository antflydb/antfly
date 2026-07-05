from enum import Enum


class RepairIssueListRequestTarget(str, Enum):
    ARTIFACT = "artifact"

    def __str__(self) -> str:
        return str(self.value)
