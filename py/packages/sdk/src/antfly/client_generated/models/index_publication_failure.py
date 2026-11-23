from enum import StrEnum


class IndexPublicationFailure(StrEnum):
    DUPLICATE_KEY = "duplicate_key"
    INVALID_CANDIDATE = "invalid_candidate"
    REQUESTED = "requested"

    def __str__(self) -> str:
        return str(self.value)
