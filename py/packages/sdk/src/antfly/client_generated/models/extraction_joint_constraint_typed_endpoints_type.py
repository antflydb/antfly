from enum import StrEnum


class ExtractionJointConstraintTypedEndpointsType(StrEnum):
    TYPEDENDPOINTS = "TypedEndpoints"

    def __str__(self) -> str:
        return str(self.value)
