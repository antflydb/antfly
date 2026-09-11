from enum import StrEnum


class ExtractionSolverStatusStatus(StrEnum):
    FEASIBLE = "feasible"
    OPTIMAL = "optimal"

    def __str__(self) -> str:
        return str(self.value)
