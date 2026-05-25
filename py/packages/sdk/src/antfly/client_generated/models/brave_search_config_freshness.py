from enum import Enum


class BraveSearchConfigFreshness(str, Enum):
    PD = "pd"
    PM = "pm"
    PW = "pw"
    PY = "py"

    def __str__(self) -> str:
        return str(self.value)
