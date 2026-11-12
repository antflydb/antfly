from enum import StrEnum


class RetainedSubscriptionFrameEventsItemType(StrEnum):
    REMOVE = "remove"
    UPSERT = "upsert"

    def __str__(self) -> str:
        return str(self.value)
