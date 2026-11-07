from enum import StrEnum


class DocumentSourceMediaType(StrEnum):
    APPLICATIONPDF = "application/pdf"
    APPLICATIONVND_OPENXMLFORMATS_OFFICEDOCUMENT_PRESENTATIONML_PRESENTATION = (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    )
    AUDIOAAC = "audio/aac"
    AUDIOFLAC = "audio/flac"
    AUDIOMP4 = "audio/mp4"
    AUDIOMPEG = "audio/mpeg"
    AUDIOOGG = "audio/ogg"
    AUDIOWAV = "audio/wav"
    AUDIOWEBM = "audio/webm"
    IMAGEJPEG = "image/jpeg"
    IMAGEPNG = "image/png"
    IMAGEWEBP = "image/webp"
    VIDEOMP4 = "video/mp4"
    VIDEOQUICKTIME = "video/quicktime"
    VIDEOWEBM = "video/webm"

    def __str__(self) -> str:
        return str(self.value)
