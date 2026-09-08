from enum import StrEnum


class ExtensionObjectKind(StrEnum):
    A2A_AGENT = "a2a_agent"
    AGENT = "agent"
    API_ENDPOINT = "api_endpoint"
    AUTH_POLICY = "auth_policy"
    CONNECTOR = "connector"
    DATA_SHAPE = "data_shape"
    ENRICHMENT = "enrichment"
    EXTENSION_RELATION = "extension_relation"
    GENERATED_ARTIFACT = "generated_artifact"
    INDEX = "index"
    INDEX_BACKEND = "index_backend"
    MAINTENANCE_TASK = "maintenance_task"
    MCP_TOOL = "mcp_tool"
    PROVIDER_ADAPTER = "provider_adapter"
    PROVIDER_CONFIG = "provider_config"
    QUERY_FUNCTION = "query_function"
    RESOLVER = "resolver"
    SKILL = "skill"
    TABLE_SCHEMA = "table_schema"
    TEXT_ANALYZER = "text_analyzer"
    TEXT_TOKENIZER = "text_tokenizer"
    WORKFLOW = "workflow"

    def __str__(self) -> str:
        return str(self.value)
