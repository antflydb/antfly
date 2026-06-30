from enum import Enum


class GraphMetricFilterOp(str, Enum):
    VALUE_0 = ">"
    VALUE_1 = ">="
    VALUE_2 = "<"
    VALUE_3 = "<="
    VALUE_4 = "="
    VALUE_5 = "=="
    VALUE_6 = "!="

    def __str__(self) -> str:
        return str(self.value)
