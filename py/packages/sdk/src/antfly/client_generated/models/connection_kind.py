from enum import Enum


class ConnectionKind(str, Enum):
    INFERENCE_PROVIDER = "inference_provider"
    OBJECT_STORE = "object_store"
    REMOTE_CONTENT_HTTP = "remote_content_http"

    def __str__(self) -> str:
        return str(self.value)
