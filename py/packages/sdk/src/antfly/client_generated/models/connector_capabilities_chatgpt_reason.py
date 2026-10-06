from enum import StrEnum


class ConnectorCapabilitiesChatgptReason(StrEnum):
    LOCAL_RUNTIME_REQUIRED = "local_runtime_required"
    OPERATOR_DISABLED = "operator_disabled"

    def __str__(self) -> str:
        return str(self.value)
