from enum import Enum


class SecretStatus(str, Enum):
    CONFIGURED_BOTH = "configured_both"
    CONFIGURED_ENV = "configured_env"
    CONFIGURED_KEYSTORE = "configured_keystore"

    def __str__(self) -> str:
        return str(self.value)
