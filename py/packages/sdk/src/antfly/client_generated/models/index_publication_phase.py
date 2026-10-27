from enum import StrEnum


class IndexPublicationPhase(StrEnum):
    BUILDING = "building"
    CANCELED = "canceled"
    CANCELING = "canceling"
    CANCEL_RELEASING = "cancel_releasing"
    FENCING = "fencing"
    INSTALLING = "installing"
    PUBLISHED = "published"
    RELEASING = "releasing"
    RESERVING = "reserving"

    def __str__(self) -> str:
        return str(self.value)
