from enum import StrEnum


class BraveSearchConfigFreshness(StrEnum):
    PD = "pd"
    PM = "pm"
    PW = "pw"
    PY = "py"

    def __str__(self) -> str:
        return str(self.value)
