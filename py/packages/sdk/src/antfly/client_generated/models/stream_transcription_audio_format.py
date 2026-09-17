from enum import StrEnum


class StreamTranscriptionAudioFormat(StrEnum):
    PCM16 = "pcm16"
    PCM_F32 = "pcm_f32"

    def __str__(self) -> str:
        return str(self.value)
