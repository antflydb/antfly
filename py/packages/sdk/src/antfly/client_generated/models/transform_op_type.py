from enum import Enum


class TransformOpType(str, Enum):
    VALUE_0 = "$set"
    VALUE_1 = "$setOnInsert"
    VALUE_10 = "$max"
    VALUE_11 = "$currentDate"
    VALUE_12 = "$rename"
    VALUE_2 = "$unset"
    VALUE_3 = "$inc"
    VALUE_4 = "$push"
    VALUE_5 = "$pull"
    VALUE_6 = "$addToSet"
    VALUE_7 = "$pop"
    VALUE_8 = "$mul"
    VALUE_9 = "$min"

    def __str__(self) -> str:
        return str(self.value)
