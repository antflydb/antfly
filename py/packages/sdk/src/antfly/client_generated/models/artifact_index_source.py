from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="ArtifactIndexSource")


@_attrs_define
class ArtifactIndexSource:
    """Named generated artifact stream consumed by an index. Producer inputs such as field and template belong on the
    matching enrichment, not on the index source.

        Attributes:
            artifact (str): Stable name of the generated artifact.
    """

    artifact: str

    def to_dict(self) -> dict[str, Any]:
        artifact = self.artifact

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "artifact": artifact,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        artifact = d.pop("artifact")

        artifact_index_source = cls(
            artifact=artifact,
        )

        return artifact_index_source
