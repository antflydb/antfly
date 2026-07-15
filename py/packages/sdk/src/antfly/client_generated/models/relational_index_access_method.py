from enum import Enum


class RelationalIndexAccessMethod(str, Enum):
    ALGEBRAIC_FILTER = "algebraic_filter"
    ORDERED_TUPLE = "ordered_tuple"
    SCALAR_COLUMN = "scalar_column"
    TEXT_SEARCH = "text_search"

    def __str__(self) -> str:
        return str(self.value)
