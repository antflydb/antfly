from enum import Enum


class EmbeddingsIndexConfigFormat(str, Enum):
    LSM_PACKED = "lsm_packed"
    SEGMENTS_BASE_DELTA = "segments_base_delta"

    def __str__(self) -> str:
        return str(self.value)
