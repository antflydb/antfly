from enum import Enum


class QueryStrategy(str, Enum):
    DECOMPOSE = "decompose"
    HYDE = "hyde"
    SIMPLE = "simple"
    STEP_BACK = "step_back"

    def __str__(self) -> str:
        return str(self.value)
