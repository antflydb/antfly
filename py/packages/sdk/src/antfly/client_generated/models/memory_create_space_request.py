from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_space_config import MemorySpaceConfig


T = TypeVar("T", bound="MemoryCreateSpaceRequest")


@_attrs_define
class MemoryCreateSpaceRequest:
    """
    Attributes:
        space (str):
        config (MemorySpaceConfig | Unset):
    """

    space: str
    config: MemorySpaceConfig | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        space = self.space

        config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.config, Unset):
            config = self.config.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "space": space,
            }
        )
        if config is not UNSET:
            field_dict["config"] = config

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_space_config import MemorySpaceConfig

        d = dict(src_dict)
        space = d.pop("space")

        _config = d.pop("config", UNSET)
        config: MemorySpaceConfig | Unset
        if isinstance(_config, Unset):
            config = UNSET
        else:
            config = MemorySpaceConfig.from_dict(_config)

        memory_create_space_request = cls(
            space=space,
            config=config,
        )

        return memory_create_space_request
