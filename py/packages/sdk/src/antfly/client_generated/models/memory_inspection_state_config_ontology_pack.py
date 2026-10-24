from enum import StrEnum


class MemoryInspectionStateConfigOntologyPack(StrEnum):
    NONE = "none"
    SOFTWARE_TEAM_V1 = "software_team_v1"

    def __str__(self) -> str:
        return str(self.value)
