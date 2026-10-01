from enum import StrEnum


class InferenceTranscriptionAudioFormat(StrEnum):
    AUTO = "auto"
    PCM16 = "pcm16"
    PCM_F32 = "pcm_f32"

    def __str__(self) -> str:
        return str(self.value)
