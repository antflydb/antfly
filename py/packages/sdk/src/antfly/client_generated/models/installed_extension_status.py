from enum import StrEnum


class InstalledExtensionStatus(StrEnum):
    DISABLED = "disabled"
    DROPPING = "dropping"
    ERROR_STATE = "error_state"
    INSTALLING = "installing"
    READY = "ready"
    UPDATING = "updating"

    def __str__(self) -> str:
        return str(self.value)
