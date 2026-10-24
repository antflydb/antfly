from enum import StrEnum


class MemoryObservationRequestOriginType0ApproverType0Kind(StrEnum):
    AGENT = "agent"
    SERVICE = "service"
    USER = "user"

    def __str__(self) -> str:
        return str(self.value)
