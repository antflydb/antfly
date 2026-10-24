from enum import StrEnum


class MemorySpaceConfigOntologyPack(StrEnum):
    NONE = "none"
    SOFTWARE_TEAM_V1 = "software_team_v1"

    def __str__(self) -> str:
        return str(self.value)
