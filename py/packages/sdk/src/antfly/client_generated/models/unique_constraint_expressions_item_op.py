from enum import Enum


class UniqueConstraintExpressionsItemOp(str, Enum):
    LOWER = "lower"
    MD5 = "md5"
    UPPER = "upper"

    def __str__(self) -> str:
        return str(self.value)
