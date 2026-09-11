from enum import StrEnum


class TransformOpType(StrEnum):
    SET = "$set"
    SET_ON_INSERT = "$setOnInsert"
    UNSET = "$unset"
    INC = "$inc"
    PUSH = "$push"
    PULL = "$pull"
    ADD_TO_SET = "$addToSet"
    MIN = "$min"
    MAX = "$max"

    def __str__(self) -> str:
        return str(self.value)
