from enum import StrEnum


class InferenceReadResultKind(StrEnum):
    IMAGE = "image"
    PAGE = "page"
    SLIDE = "slide"

    def __str__(self) -> str:
        return str(self.value)
