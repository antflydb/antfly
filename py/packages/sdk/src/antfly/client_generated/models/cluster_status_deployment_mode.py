from enum import StrEnum


class ClusterStatusDeploymentMode(StrEnum):
    DISTRIBUTED = "distributed"
    EMBEDDED = "embedded"
    SERVERLESS = "serverless"
    STANDALONE = "standalone"

    def __str__(self) -> str:
        return str(self.value)
