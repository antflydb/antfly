from enum import StrEnum


class GeneratedDecideRequestDecisionMethod(StrEnum):
    GENERATED = "generated"

    def __str__(self) -> str:
        return str(self.value)
