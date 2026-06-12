from enum import Enum


class ObjectStoreConnectionPurpose(str, Enum):
    INFERENCE_MODELS = "inference_models"
    REMOTE_CONTENT = "remote_content"
    STORAGE = "storage"

    def __str__(self) -> str:
        return str(self.value)
