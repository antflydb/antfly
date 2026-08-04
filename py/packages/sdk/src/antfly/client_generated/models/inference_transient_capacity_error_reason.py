from enum import Enum


class InferenceTransientCapacityErrorReason(str, Enum):
    INFERENCE_CAPACITY = "inference_capacity"
    REQUEST_QUEUE = "request_queue"

    def __str__(self) -> str:
        return str(self.value)
