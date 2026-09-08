from enum import StrEnum


class RepairRunRequestControl(StrEnum):
    CANCEL_CURRENT_ATTEMPT = "cancel_current_attempt"
    PAUSE_AUTOMATIC = "pause_automatic"
    RESUME_AUTOMATIC = "resume_automatic"

    def __str__(self) -> str:
        return str(self.value)
