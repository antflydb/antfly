from enum import StrEnum


class MemoryObservationRequestOriginType0TraceType0Kind(StrEnum):
    ASSERTION = "assertion"
    CONFIGURATION = "configuration"
    DECISION = "decision"
    ENTITY = "entity"
    EVIDENCE = "evidence"
    OPERATION = "operation"
    OUTCOME = "outcome"
    RECORD = "record"
    RELATIONSHIP = "relationship"
    TABLE = "table"
    TRACE = "trace"

    def __str__(self) -> str:
        return str(self.value)
