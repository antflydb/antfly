from enum import StrEnum


class MemoryObservationRequestOriginType0ApproverType0Assurance(StrEnum):
    AUTHENTICATED = "authenticated"
    DECLARED = "declared"

    def __str__(self) -> str:
        return str(self.value)
