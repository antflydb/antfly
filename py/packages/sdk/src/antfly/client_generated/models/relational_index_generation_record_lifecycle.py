from enum import Enum


class RelationalIndexGenerationRecordLifecycle(str, Enum):
    BUILDING = "building"
    CATCHING_UP = "catching_up"
    DROPPING = "dropping"
    FAILED = "failed"
    INVALID = "invalid"
    READY = "ready"
    REBUILD_REQUIRED = "rebuild_required"
    STALE = "stale"

    def __str__(self) -> str:
        return str(self.value)
