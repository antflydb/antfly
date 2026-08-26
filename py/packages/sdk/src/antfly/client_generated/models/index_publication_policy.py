from enum import Enum


class IndexPublicationPolicy(str, Enum):
    ATOMIC = "atomic"
    PROGRESSIVE = "progressive"

    def __str__(self) -> str:
        return str(self.value)
