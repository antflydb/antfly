from enum import Enum


class TransformOpType(str, Enum):
    VALUE_0 = "$set"
    VALUE_1 = "$unset"
    VALUE_10 = "$currentDate"
    VALUE_11 = "$rename"
    VALUE_2 = "$inc"
    VALUE_3 = "$push"
    VALUE_4 = "$pull"
    VALUE_5 = "$addToSet"
    VALUE_6 = "$pop"
    VALUE_7 = "$mul"
    VALUE_8 = "$min"
    VALUE_9 = "$max"

    def __str__(self) -> str:
        return str(self.value)
