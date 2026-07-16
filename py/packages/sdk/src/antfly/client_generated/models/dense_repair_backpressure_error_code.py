from enum import Enum


class DenseRepairBackpressureErrorCode(str, Enum):
    DENSE_REPAIR_BACKPRESSURE = "dense_repair_backpressure"

    def __str__(self) -> str:
        return str(self.value)
