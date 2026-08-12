from enum import Enum


class TransformOpType(str, Enum):
    SET = "$set"
    SET_ON_INSERT = "$setOnInsert"
    UNSET = "$unset"
    INC = "$inc"
    PUSH = "$push"
    ADD_TO_SET = "$addToSet"
    MIN = "$min"
    MAX = "$max"

    def __str__(self) -> str:
        return str(self.value)
