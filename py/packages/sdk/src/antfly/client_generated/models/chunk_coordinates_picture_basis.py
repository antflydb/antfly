from enum import StrEnum


class ChunkCoordinatesPictureBasis(StrEnum):
    LOGICAL = "logical"
    PACKET_ASSEMBLY = "packet_assembly"

    def __str__(self) -> str:
        return str(self.value)
