from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphTtlConfig")


@_attrs_define
class GraphTtlConfig:
    """
    Attributes:
        duration (str): Expiration duration using Antfly's integer-component duration format (ns, us, ms, s, m, h, d).
    """

    duration: str

    def to_dict(self) -> dict[str, Any]:
        duration = self.duration

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "duration": duration,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        duration = d.pop("duration")

        graph_ttl_config = cls(
            duration=duration,
        )

        return graph_ttl_config
