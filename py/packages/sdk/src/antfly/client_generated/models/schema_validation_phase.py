from enum import StrEnum


class SchemaValidationPhase(StrEnum):
    CANCELED = "canceled"
    CANCELING = "canceling"
    INSTALLED = "installed"
    INSTALLING = "installing"
    RELEASING = "releasing"
    SUCCEEDED = "succeeded"
    VALIDATED = "validated"
    VALIDATING = "validating"

    def __str__(self) -> str:
        return str(self.value)
