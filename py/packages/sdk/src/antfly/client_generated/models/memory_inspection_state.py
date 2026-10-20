from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_inspection_state_config import MemoryInspectionStateConfig
    from ..models.memory_inspection_state_pending_config_type_0 import MemoryInspectionStatePendingConfigType0


T = TypeVar("T", bound="MemoryInspectionState")


@_attrs_define
class MemoryInspectionState:
    """
    Attributes:
        database (str):
        namespace (str):
        space (str):
        config (MemoryInspectionStateConfig):
        pending_config (MemoryInspectionStatePendingConfigType0 | None):
        revision (int):
        owner (str):
        created_at (int):
    """

    database: str
    namespace: str
    space: str
    config: MemoryInspectionStateConfig
    pending_config: MemoryInspectionStatePendingConfigType0 | None
    revision: int
    owner: str
    created_at: int

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_inspection_state_pending_config_type_0 import MemoryInspectionStatePendingConfigType0

        database = self.database

        namespace = self.namespace

        space = self.space

        config = self.config.to_dict()

        pending_config: dict[str, Any] | None
        if isinstance(self.pending_config, MemoryInspectionStatePendingConfigType0):
            pending_config = self.pending_config.to_dict()
        else:
            pending_config = self.pending_config

        revision = self.revision

        owner = self.owner

        created_at = self.created_at

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "database": database,
                "namespace": namespace,
                "space": space,
                "config": config,
                "pending_config": pending_config,
                "revision": revision,
                "owner": owner,
                "created_at": created_at,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_inspection_state_config import MemoryInspectionStateConfig
        from ..models.memory_inspection_state_pending_config_type_0 import MemoryInspectionStatePendingConfigType0

        d = dict(src_dict)
        database = d.pop("database")

        namespace = d.pop("namespace")

        space = d.pop("space")

        config = MemoryInspectionStateConfig.from_dict(d.pop("config"))

        def _parse_pending_config(data: object) -> MemoryInspectionStatePendingConfigType0 | None:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                pending_config_type_0 = MemoryInspectionStatePendingConfigType0.from_dict(data)

                return pending_config_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryInspectionStatePendingConfigType0 | None, data)

        pending_config = _parse_pending_config(d.pop("pending_config"))

        revision = d.pop("revision")

        owner = d.pop("owner")

        created_at = d.pop("created_at")

        memory_inspection_state = cls(
            database=database,
            namespace=namespace,
            space=space,
            config=config,
            pending_config=pending_config,
            revision=revision,
            owner=owner,
            created_at=created_at,
        )

        return memory_inspection_state
