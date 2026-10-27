from enum import StrEnum


class DocumentSourceMediaType(StrEnum):
    APPLICATIONPDF = "application/pdf"
    APPLICATIONVND_OPENXMLFORMATS_OFFICEDOCUMENT_PRESENTATIONML_PRESENTATION = (
        "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    )
    IMAGEJPEG = "image/jpeg"
    IMAGEPNG = "image/png"
    IMAGEWEBP = "image/webp"

    def __str__(self) -> str:
        return str(self.value)
