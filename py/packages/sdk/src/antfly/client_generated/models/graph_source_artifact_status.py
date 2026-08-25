from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_source_artifact_status_format import GraphSourceArtifactStatusFormat

T = TypeVar("T", bound="GraphSourceArtifactStatus")


@_attrs_define
class GraphSourceArtifactStatus:
    """Configured graph artifact source projected in deterministic precedence order.

    Attributes:
        artifact (str): Canonical artifact source identity.
        path (str):
        format_ (GraphSourceArtifactStatusFormat):
    """

    artifact: str
    path: str
    format_: GraphSourceArtifactStatusFormat

    def to_dict(self) -> dict[str, Any]:
        artifact = self.artifact

        path = self.path

        format_ = self.format_.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "artifact": artifact,
                "path": path,
                "format": format_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        artifact = d.pop("artifact")

        path = d.pop("path")

        format_ = GraphSourceArtifactStatusFormat(d.pop("format"))

        graph_source_artifact_status = cls(
            artifact=artifact,
            path=path,
            format_=format_,
        )

        return graph_source_artifact_status
