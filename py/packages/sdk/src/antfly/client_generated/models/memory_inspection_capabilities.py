from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="MemoryInspectionCapabilities")


@_attrs_define
class MemoryInspectionCapabilities:
    """
    Attributes:
        native (bool):
        model_free (bool):
        generator_available (bool):
        embedder_available (bool):
    """

    native: bool
    model_free: bool
    generator_available: bool
    embedder_available: bool

    def to_dict(self) -> dict[str, Any]:
        native = self.native

        model_free = self.model_free

        generator_available = self.generator_available

        embedder_available = self.embedder_available

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "native": native,
                "model_free": model_free,
                "generator_available": generator_available,
                "embedder_available": embedder_available,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        native = d.pop("native")

        model_free = d.pop("model_free")

        generator_available = d.pop("generator_available")

        embedder_available = d.pop("embedder_available")

        memory_inspection_capabilities = cls(
            native=native,
            model_free=model_free,
            generator_available=generator_available,
            embedder_available=embedder_available,
        )

        return memory_inspection_capabilities
