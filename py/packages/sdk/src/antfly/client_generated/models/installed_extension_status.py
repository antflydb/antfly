from enum import Enum


class InstalledExtensionStatus(str, Enum):
    DISABLED = "disabled"
    DROPPING = "dropping"
    ERROR_STATE = "error_state"
    INSTALLING = "installing"
    READY = "ready"
    UPDATING = "updating"

    def __str__(self) -> str:
        return str(self.value)
