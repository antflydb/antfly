from enum import Enum


class ClusterStatusDeploymentMode(str, Enum):
    DISTRIBUTED = "distributed"
    EMBEDDED = "embedded"
    SERVERLESS = "serverless"
    STANDALONE = "standalone"

    def __str__(self) -> str:
        return str(self.value)
