from enum import StrEnum


class GraphWireValueType(StrEnum):
    BOOLEAN = "boolean"
    INTEGER = "integer"
    LIST = "list"
    MAP = "map"
    NODE = "node"
    NULL = "null"
    NUMBER = "number"
    PATH = "path"
    RELATIONSHIP = "relationship"
    STRING = "string"

    def __str__(self) -> str:
        return str(self.value)
