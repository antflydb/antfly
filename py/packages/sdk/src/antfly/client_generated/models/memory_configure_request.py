from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_space_config import MemorySpaceConfig


T = TypeVar("T", bound="MemoryConfigureRequest")


@_attrs_define
class MemoryConfigureRequest:
    """
    Attributes:
        expected_revision (int):
        config (MemorySpaceConfig):
    """

    expected_revision: int
    config: MemorySpaceConfig

    def to_dict(self) -> dict[str, Any]:
        expected_revision = self.expected_revision

        config = self.config.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "expected_revision": expected_revision,
                "config": config,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_space_config import MemorySpaceConfig

        d = dict(src_dict)
        expected_revision = d.pop("expected_revision")

        config = MemorySpaceConfig.from_dict(d.pop("config"))

        memory_configure_request = cls(
            expected_revision=expected_revision,
            config=config,
        )

        return memory_configure_request
