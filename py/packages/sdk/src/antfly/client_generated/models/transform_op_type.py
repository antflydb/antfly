from enum import Enum


class TransformOpType(str, Enum):
    VALUE_0 = "$set"
    VALUE_1 = "$setOnInsert"
    VALUE_2 = "$unset"
    VALUE_3 = "$inc"
    VALUE_4 = "$addToSet"
    VALUE_5 = "$max"

    def __str__(self) -> str:
        return str(self.value)
