from enum import StrEnum


class RetainedSubscriptionFrameInitialHistory(StrEnum):
    CERTIFIED = "certified"
    RETAINED_SOURCE_ROWS = "retained_source_rows"

    def __str__(self) -> str:
        return str(self.value)
