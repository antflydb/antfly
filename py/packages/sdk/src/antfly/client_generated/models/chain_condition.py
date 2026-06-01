from enum import Enum


class ChainCondition(str, Enum):
    ALWAYS = "always"
    ON_ERROR = "on_error"
    ON_RATE_LIMIT = "on_rate_limit"
    ON_TIMEOUT = "on_timeout"

    def __str__(self) -> str:
        return str(self.value)
