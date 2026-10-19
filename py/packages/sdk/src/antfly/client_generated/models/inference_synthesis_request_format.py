from enum import StrEnum


class InferenceSynthesisRequestFormat(StrEnum):
    AAC = "aac"
    FLAC = "flac"
    MP3 = "mp3"
    OPUS = "opus"
    PCM = "pcm"
    WAV = "wav"

    def __str__(self) -> str:
        return str(self.value)
