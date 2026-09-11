from enum import StrEnum


class IndexPublicationPolicy(StrEnum):
    ATOMIC = "atomic"
    PROGRESSIVE = "progressive"

    def __str__(self) -> str:
        return str(self.value)
