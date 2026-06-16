from enum import Enum


class DataShapeKind(str, Enum):
    DOCUMENT = "document"
    ENDPOINT_SCHEMA = "endpoint_schema"
    EXTENSION_RELATION = "extension_relation"
    GENERATED_ARTIFACT = "generated_artifact"
    ROW = "row"
    TOOL_SCHEMA = "tool_schema"

    def __str__(self) -> str:
        return str(self.value)
