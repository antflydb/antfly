from enum import StrEnum


class DenseRepairBackpressureErrorCode(StrEnum):
    DENSE_REPAIR_BACKPRESSURE = "dense_repair_backpressure"

    def __str__(self) -> str:
        return str(self.value)
