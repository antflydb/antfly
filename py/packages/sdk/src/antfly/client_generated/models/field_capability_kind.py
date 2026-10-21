from enum import StrEnum


class FieldCapabilityKind(StrEnum):
    COMPUTED = "computed"
    RESERVED = "reserved"
    SOURCE = "source"
    TEMPLATE = "template"
    UNASSOCIATED = "unassociated"

    def __str__(self) -> str:
        return str(self.value)
